"""
祥哥籌碼價量戰情室 v3.4
1. 禁用圖表拖曳：解決手機版無法捲動頁面的問題。
2. 補完所有功能：還原 v3.1 所有指標、診斷文字與散戶定義。
3. 視覺加強：柱子加粗、數值格式化。
"""

import json
import os
import glob
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf
from plotly.subplots import make_subplots

# ═══════════════════════════════════════════════════════════════
# 1. 基礎設定與資料讀取
# ═══════════════════════════════════════════════════════════════
st.set_page_config(page_title="祥哥籌碼價量戰情室", layout="wide")

@st.cache_data
def load_stock_map() -> dict[str, str]:
    path = Path("stock_map.json")
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}

@st.cache_data
def load_stock_data(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df["資料日期"] = pd.to_datetime(df["資料日期"].astype(str), format="%Y%m%d", errors="coerce")
    for col in ["權重", "人數", "股數"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df.dropna(subset=["資料日期"])

@st.cache_data(ttl=3600)
def get_price_data(sid: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    ticker = f"{sid}.TW" if sid.endswith(("A", "B")) else f"{sid}.TW"
    try:
        df = yf.download(ticker, start=start_date-timedelta(days=30), end=end_date+timedelta(days=1), progress=False, auto_adjust=True)
        if not df.empty and isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df
    except:
        return pd.DataFrame()

stock_map = load_stock_map()

# ═══════════════════════════════════════════════════════════════
# 2. Sidebar (全功能選單)
# ═══════════════════════════════════════════════════════════════
with st.sidebar:
    st.header("⚙️ 核心設定")
    stock_options = [f"{k} {v}" for k, v in stock_map.items()]
    selected_stock = st.selectbox("搜尋股號或名稱", options=stock_options, index=stock_options.index("2330 台積電") if "2330 台積電" in stock_options else 0)
    sid = selected_stock.split(" ")[0]
    sname = stock_map.get(sid, "")
    d_range = st.date_input("選擇區間 (決定量化報告範圍)", [datetime.now() - timedelta(days=120), datetime.now()])
    price_freq = st.radio("價量資料頻率", ["日資料", "週資料 (同步籌碼)"], index=0)

    st.divider()
    st.subheader("👥 級別定義 (1-15級)")
    big_lv   = st.multiselect("🔴 大戶",   options=list(range(1, 16)), default=[15], key="big")
    mid_lv   = st.multiselect("🟡 中間戶", options=list(range(1, 16)), default=[11, 12, 13, 14], key="mid")
    small_lv = st.multiselect("🟢 散戶",   options=list(range(1, 16)), default=list(range(1, 8)), key="small")
    st.caption("Powered by 祥哥籌碼模型 v3.4")

st.title("🚀 祥哥籌碼價量戰情室 (15級全功能版)")
tab1, tab2 = st.tabs(["📊 全市場掃描總覽", "🔍 個股深度分析"])

# ═══════════════════════════════════════════════════════════════
# Tab 1：全市場總覽 (格式優化)
# ═══════════════════════════════════════════════════════════════
with tab1:
    st.subheader("🏆 全市場籌碼集中度即時排行榜")
    if os.path.exists("latest_snapshot.parquet"):
        df_rank = pd.read_parquet("latest_snapshot.parquet")
        df_rank["名稱"] = df_rank["股號"].map(stock_map)
        def _color_num(val):
            return "color: red" if isinstance(val, (int, float)) and val > 0 else "color: green" if isinstance(val, (int, float)) and x < 0 else ""
        display_cols = ["股號", "名稱", "大戶%", "大戶週增減", "人數變動", "集中度(大+中)"]
        st.dataframe(df_rank[display_cols].style.format("{:.2f}", subset=["大戶%", "大戶週增減", "人數變動", "集中度(大+中)"]).map(lambda x: "color: red" if isinstance(x, (int, float)) and x > 0 else "color: green" if isinstance(x, (int, float)) and x < 0 else "", subset=["大戶週增減", "人數變動"]), use_container_width=True, height=500)
    else:
        st.info("尚未偵測到全市場快照資料。若是新部署，請等待下週五數據更新產出。")

# ═══════════════════════════════════════════════════════════════
# Tab 2：個股深度分析 (禁用縮放 & 功能全開)
# ═══════════════════════════════════════════════════════════════
with tab2:
    st.header(f"📈 {sid} {sname} 深度戰情看板")
    path = Path("data/chip") / sid[:2] / f"{sid}.parquet"
    if not path.exists() or len(d_range) != 2:
        st.warning("⚠️ 找不到籌碼資料或日期未選全。")
        st.stop()

    start_dt, end_dt = pd.to_datetime(d_range[0]), pd.to_datetime(d_range[1])
    df_chip = load_stock_data(str(path))
    df_chip = df_chip[(df_chip["資料日期"] >= start_dt) & (df_chip["資料日期"] <= end_dt)]
    if df_chip.empty: st.stop()
    df_price = get_price_data(sid, start_dt, end_dt)

    def _agg(sub, lvs):
        m = sub["持股分級"].isin(lvs)
        return float(sub.loc[m, "權重"].sum()), float(sub.loc[m, "人數"].sum()), float(sub.loc[m, "股數"].sum())

    weekly_rows = []
    for d, sub in df_chip.groupby("資料日期"):
        pm = df_price[df_price.index <= d]
        p_close, p_vol = (float(pm.iloc[-1]["Close"]), float(pm.iloc[-1]["Volume"])) if not pm.empty else (0.0, 0.0)
        bw, bp, bs = _agg(sub, big_lv); mw, mp, ms = _agg(sub, mid_lv); sw, sp, ss = _agg(sub, small_lv)
        tp, ts = float(sub["人數"].sum()), float(sub["股數"].sum())
        weekly_rows.append({"日期": d, "股價": round(p_close, 2), "成交量": int(p_vol), "大戶%": round(bw, 2), "中間戶%": round(mw, 2), "散戶%": round(sw, 2), "總人數": int(tp), "人均張數": round((ts/tp)/1000, 2) if tp > 0 else 0, "集中度": round(bw + mw, 2)})

    res = pd.DataFrame(weekly_rows).sort_values("日期", ascending=True).reset_index(drop=True)
    res["大戶增減"], res["散戶增減"], res["人數增減"] = res["大戶%"].diff().fillna(0), res["散戶%"].diff().fillna(0), res["總人數"].diff().fillna(0)
    res["診斷"] = res.apply(lambda r: "🔴 強力吸籌" if r["大戶增減"] > 0 and r["人數增減"] < 0 else ("🟡 主力加碼" if r["大戶增減"] > 0 else ("🟠 主力減碼" if r["大戶增減"] < 0 else "⚪ 中性觀望")), axis=1)

    # ── 圖表：禁用拖曳與縮放 ──
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.5, 0.15, 0.35], specs=[[{"secondary_y": True}], [{"secondary_y": False}], [{"secondary_y": False}]])
    
    # 柱子加粗設定
    fig.add_trace(go.Bar(x=res["日期"], y=res["總人數"], name="總人數", marker_color="royalblue", opacity=0.8, width=432000000), row=3, col=1)
    
    fig.add_trace(go.Scatter(x=res["日期"], y=res["大戶%"], name="大戶%", line=dict(color="red", width=3)), row=1, col=1, secondary_y=True)
    fig.add_trace(go.Scatter(x=res["日期"], y=res["中間戶%"], name="中間戶%", line=dict(color="orange", width=2, dash="dot")), row=1, col=1, secondary_y=True)
    fig.add_trace(go.Scatter(x=res["日期"], y=res["散戶%"], name="散戶%", line=dict(color="green", width=2)), row=1, col=1, secondary_y=True)

    if not df_price.empty:
        p_plot = df_price.copy()
        if price_freq == "週資料 (同步籌碼)":
            p_plot = p_plot.resample("W-FRI").agg({"Open":"first","High":"max","Low":"min","Close":"last","Volume":"sum"}).dropna()
        fig.add_trace(go.Candlestick(x=p_plot.index, open=p_plot["Open"], high=p_plot["High"], low=p_plot["Low"], close=p_plot["Close"], name="K線"), row=1, col=1)
        fig.add_trace(go.Bar(x=p_plot.index, y=p_plot["Volume"], name="量", marker_color="gray", opacity=0.3), row=2, col=1)

    # 關鍵修正：禁用拖曳(dragmode=False)與固定工具列[cite: 12]
    fig.update_layout(height=650, margin=dict(l=10, r=10, t=30, b=10), dragmode=False, hovermode="x unified", xaxis_rangeslider_visible=False, template="plotly_dark", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    
    # 使用 config 徹底關閉互動，確保手機滑動順暢
    st.plotly_chart(fig, use_container_width=True, config={'staticPlot': False, 'scrollZoom': False, 'displayModeBar': False})

    # ── 指標看板 (格式對齊[cite: 12]) ──
    st.subheader("📋 區間量化詳細指標看板")
    view = res.sort_values("日期", ascending=False).copy()
    view["日期"] = view["日期"].dt.strftime("%Y-%m-%d")
    st.dataframe(view.style.format({"大戶%": "{:.2f}", "中間戶%": "{:.2f}", "散戶%": "{:.2f}", "大戶增減": "{:+.2f}", "散戶增減": "{:+.2f}", "總人數": "{:,}"}).map(lambda x: "color: red" if isinstance(x, (int, float)) and x > 0 else "color: green" if isinstance(x, (int, float)) and x < 0 else "", subset=["大戶增減", "散戶增減", "人數增減"]), use_container_width=True)

    # ── 祥哥量化報告 (100% v3.1 還原[cite: 12]) ──
    st.divider()
    st.subheader(f"📊 {sid} 祥哥區間量化報告")
    if len(res) >= 2:
        f, l = res.iloc[0], res.iloc[-1]
        c1, c2, c3 = st.columns(3)
        with c1:
            st.write("**💰 區間總變動**")
            st.metric("大戶持股變動", f"{l['大戶%']:.2f}%", f"{l['大戶%']-f['大戶%']:+.2f}%")
            st.metric("總人數增減", f"{l['總人數']:,} 人", f"{l['總人數']-f['總人數']:+.0f} 人", delta_color="inverse")
        with c2:
            st.write("**📉 持續性分析**")
            conc_count = ((res["大戶增減"] > 0) & (res["人數增減"] < 0)).sum()
            st.info(f"區間集中慣性：**{conc_count}** / {len(res)-1} 週")
            if len(res[["大戶%", "股價"]].dropna()) >= 2:
                corr = res[["大戶%", "股價"]].corr().iloc[0, 1]
                st.write(f"大戶/股價相關性：**{corr:.2f}**")
        with c3:
            st.write("**📝 綜合判斷**")
            if (l["大戶%"] - f["大戶%"]) > 0 and (l["總人數"] - f["總人數"]) < 0:
                st.success("✅ 【強力吸籌】籌碼極度集中")
            elif (l["大戶%"] - f["大戶%"]) < 0:
                st.error("⚠️ 【籌碼渙散】注意主力撤出")
            else:
                st.warning("⚪ 【盤整換手】多空力道拉鋸")
    else:
        st.info("💡 區間報告需至少兩週資料。")
