"""
祥哥籌碼價量戰情室 v3.2
基於 v3.1 穩定版，僅優化以下格式：
1. 總人數柱狀圖加粗
2. 表格小數點固定 2 位
3. 響應式手機版佈局優化
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf
from plotly.subplots import make_subplots

# ═══════════════════════════════════════════════════════════════
# 1. 基礎設定 (完整保留 v3.1 邏輯)
# ═══════════════════════════════════════════════════════════════
st.set_page_config(page_title="祥哥籌碼價量戰情室", layout="wide")

@st.cache_data
def load_stock_map() -> dict[str, str]:
    path = Path("stock_map.json")
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}

@st.cache_data
def load_stock_data(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df["資料日期"] = pd.to_datetime(
        df["資料日期"].astype(str), format="%Y%m%d", errors="coerce"
    )
    for col in ["權重", "人數", "股數"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df.dropna(subset=["資料日期"])

@st.cache_data(ttl=3600)
def get_price_data(sid: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    if sid.endswith(("A", "B")):
        tickers = [f"{sid}.TW"]
    else:
        tickers = [f"{sid}.TW", f"{sid}.TWO"]
    fetch_start, fetch_end = start_date - timedelta(days=30), end_date + timedelta(days=1)
    for ticker in tickers:
        try:
            df = yf.download(ticker, start=fetch_start, end=fetch_end, progress=False, auto_adjust=True)
            if df.empty: continue
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            return df
        except Exception: continue
    return pd.DataFrame()

stock_map = load_stock_map()

# ═══════════════════════════════════════════════════════════════
# 2. Sidebar (手機版樣式微調)
# ═══════════════════════════════════════════════════════════════
with st.sidebar:
    st.header("⚙️ 核心設定")
    stock_options = [f"{k} {v}" for k, v in stock_map.items()]
    default_idx = stock_options.index("2330 台積電") if "2330 台積電" in stock_options else 0
    selected_stock = st.selectbox("搜尋股號或名稱", options=stock_options, index=default_idx)
    sid, sname = selected_stock.split(" ")[0], stock_map.get(selected_stock.split(" ")[0], "")
    today = datetime.now()
    d_range = st.date_input("選擇區間", [today - timedelta(days=120), today])
    price_freq = st.radio("價量資料頻率", ["日資料", "週資料 (同步籌碼)"], index=0)

    st.divider()
    st.subheader("👥 級別定義")
    big_lv   = st.multiselect("🔴 大戶",   options=list(range(1, 16)), default=[15], key="big")
    mid_lv   = st.multiselect("🟡 中間戶", options=list(range(1, 16)), default=[11, 12, 13, 14], key="mid")
    small_lv = st.multiselect("🟢 散戶",   options=list(range(1, 16)), default=list(range(1, 8)), key="small")
    st.caption("Powered by 祥哥籌碼模型 v3.2")

# ═══════════════════════════════════════════════════════════════
# 3. 主頁面內容
# ═══════════════════════════════════════════════════════════════
st.title("🚀 祥哥籌碼價量戰情室")
tab1, tab2 = st.tabs(["📊 全市場掃描總覽", "🔍 個股深度分析"])

with tab1:
    st.subheader("🏆 全市場即時排行榜")
    if os.path.exists("latest_snapshot.parquet"):
        df_rank = pd.read_parquet("latest_snapshot.parquet")
        df_rank["名稱"] = df_rank["股號"].map(stock_map)
        st.dataframe(df_rank[["股號", "名稱", "大戶%", "大戶週增減", "人數變動", "集中度(大+中)"]].style.map(
            lambda x: "color: red" if isinstance(x, (int, float)) and x > 0 else "color: green" if isinstance(x, (int, float)) and x < 0 else "",
            subset=["大戶週增減", "人數變動"]
        ).format("{:.2f}", subset=["大戶%", "大戶週增減", "人數變動", "集中度(大+中)"]), use_container_width=True, height=600)
    else:
        st.info("尚未偵測到全市場快照資料。")

with tab2:
    st.header(f"📈 {sid} {sname} 看板")
    folder, path = Path("data/chip") / sid[:2], Path("data/chip") / sid[:2] / f"{sid}.parquet"
    if not path.exists() or len(d_range) != 2:
        st.warning("資料讀取中或起訖日期未選全...")
        st.stop()

    start_dt, end_dt = pd.to_datetime(d_range[0]), pd.to_datetime(d_range[1])
    raw_chip = load_stock_data(str(path))
    df_chip = raw_chip[(raw_chip["資料日期"] >= start_dt) & (raw_chip["資料日期"] <= end_dt)]
    if df_chip.empty: st.stop()
    df_price = get_price_data(sid, start_dt, end_dt)

    weekly_rows = []
    for d, sub in df_chip.groupby("資料日期"):
        p_close, p_vol = (0.0, 0.0) if df_price.empty or df_price[df_price.index <= d].empty else (float(df_price[df_price.index <= d].iloc[-1].get("Close", 0)), float(df_price[df_price.index <= d].iloc[-1].get("Volume", 0)))
        mask_b, mask_m, mask_s = sub["持股分級"].isin(big_lv), sub["持股分級"].isin(mid_lv), sub["持股分級"].isin(small_lv)
        tp, ts = float(sub["人數"].sum()), float(sub["股數"].sum())
        weekly_rows.append({"日期": d, "股價": round(p_close, 2), "成交量": int(p_vol), "大戶%": round(float(sub.loc[mask_b, "權重"].sum()), 2), "中間戶%": round(float(sub.loc[mask_m, "權重"].sum()), 2), "散戶%": round(float(sub.loc[mask_s, "權重"].sum()), 2), "總人數": int(tp), "人均張數": round((ts/tp)/1000, 2) if tp > 0 else 0})

    res = pd.DataFrame(weekly_rows).sort_values("日期", ascending=True).reset_index(drop=True)
    res["大戶增減"], res["散戶增減"], res["人數增減"] = res["大戶%"].diff().fillna(0), res["散戶%"].diff().fillna(0), res["總人數"].diff().fillna(0)
    res["診斷"] = res.apply(lambda r: "🔴 強力吸籌" if r["大戶增減"] > 0 and r["人數增減"] < 0 else ("🟡 主力加碼" if r["大戶增減"] > 0 else ("🟠 主力減碼" if r["大戶增減"] < 0 else "⚪ 中性觀望")), axis=1)

    # ── 圖表格式優化 ──
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.5, 0.15, 0.35], specs=[[{"secondary_y": True}], [{"secondary_y": False}], [{"secondary_y": False}]])
    plot_p = df_price.copy()
    if price_freq == "週資料 (同步籌碼)" and not plot_p.empty:
        plot_p = plot_p.resample("W-FRI").agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}).dropna(subset=["Close"])
    
    if not plot_p.empty:
        fig.add_trace(go.Candlestick(x=plot_p.index, open=plot_p["Open"], high=plot_p["High"], low=plot_p["Low"], close=plot_p["Close"], name="K線"), row=1, col=1)
        fig.add_trace(go.Bar(x=plot_p.index, y=plot_p["Volume"], name="成交量", marker_color=["red" if c >= o else "green" for o, c in zip(plot_p["Open"], plot_p["Close"])], opacity=0.4), row=2, col=1)

    fig.add_trace(go.Scatter(x=res["日期"], y=res["大戶%"], name="大戶%", line=dict(color="red", width=3)), row=1, col=1, secondary_y=True)
    fig.add_trace(go.Scatter(x=res["日期"], y=res["中間戶%"], name="中間戶%", line=dict(color="orange", width=2, dash="dot")), row=1, col=1, secondary_y=True)
    
    # 格式優化：柱狀圖變粗 (width 設為 5天)
    fig.add_trace(go.Bar(x=res["日期"], y=res["總人數"], name="總人數", marker_color="royalblue", opacity=0.8, width=5*24*60*60*1000), row=3, col=1)

    fig.update_layout(height=700, margin=dict(l=5, r=5, t=30, b=5), template="plotly_dark", hovermode="x unified", xaxis_rangeslider_visible=False, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    st.plotly_chart(fig, use_container_width=True)

    # ── 表格格式優化：固定小數點 ──
    st.subheader("📋 詳細指標 (格式優化)")
    st.dataframe(res.sort_values("日期", ascending=False).style.format({
        "大戶%": "{:.2f}", "中間戶%": "{:.2f}", "散戶%": "{:.2f}", 
        "大戶增減": "{:+.2f}", "散戶增減": "{:+.2f}", "總人數": "{:,}"
    }).map(lambda x: "color: red" if isinstance(x, (int, float)) and x > 0 else "color: green" if isinstance(x, (int, float)) and x < 0 else "", subset=["大戶增減", "散戶增減", "人數增減"]), use_container_width=True)

    # ── 報告區 ──
    if len(res) >= 2:
        f, l = res.iloc[0], res.iloc[-1]
        c1, c2, c3 = st.columns(3)
        c1.metric("大戶持股變動", f"{l['大戶%']:.2f}%", f"{l['大戶%']-f['大戶%']:+.2f}%")
        c2.metric("總人數增減", f"{l['總人數']:,} 人", f"{l['總人數']-f['總人數']:+.0f} 人", delta_color="inverse")
        if l['大戶%'] > f['大戶%'] and l['總人數'] < f['總人數']: st.success("✅ 【強力吸籌】")
