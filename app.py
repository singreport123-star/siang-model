"""
祥哥籌碼價量戰情室 v3.3
1. 自動診斷：區分「沒資料」與「資料不足兩週」。
2. RWD 優化：手機版寬度、高度適配。
3. 視覺加強：柱子加粗、小數點固定兩位。
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
# 1. 基礎邏輯 (維持 v3.1/3.2 穩定邏輯)
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
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df.dropna(subset=["資料日期"])

@st.cache_data(ttl=3600)
def get_price_data(sid: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    ticker = f"{sid}.TW" if sid.endswith(("A", "B")) else f"{sid}.TW" # 簡化邏輯
    try:
        df = yf.download(ticker, start=start_date-timedelta(days=30), end=end_date+timedelta(days=1), progress=False, auto_adjust=True)
        if not df.empty and isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
        return df
    except: return pd.DataFrame()

stock_map = load_stock_map()

# ═══════════════════════════════════════════════════════════════
# 2. Sidebar (手機版樣式壓縮)
# ═══════════════════════════════════════════════════════════════
with st.sidebar:
    st.header("⚙️ 核心設定")
    stock_options = [f"{k} {v}" for k, v in stock_map.items()]
    selected_stock = st.selectbox("搜尋標的", options=stock_options if stock_options else ["2330 台積電"])
    sid = selected_stock.split(" ")[0]
    sname = stock_map.get(sid, "")
    d_range = st.date_input("選擇區間", [datetime.now() - timedelta(days=120), datetime.now()])
    price_freq = st.radio("價量頻率", ["日資料", "週資料 (同步)"], index=0)
    st.divider()
    big_lv = st.multiselect("🔴 大戶", range(1, 16), default=[15], key="big")
    mid_lv = st.multiselect("🟡 中間戶", range(1, 16), default=[11, 12, 13, 14], key="mid")
    st.caption("Powered by 祥哥籌碼模型 v3.3")

st.title("🚀 祥哥籌碼價量戰情室")
tab1, tab2 = st.tabs(["📊 市場排行榜", "🔍 個股深度分析"])

# ═══════════════════════════════════════════════════════════════
# Tab 1：全市場排行榜 (新增診斷邏輯)
# ═══════════════════════════════════════════════════════════════
with tab1:
    st.subheader("🏆 全市場籌碼集中度排行")
    if os.path.exists("latest_snapshot.parquet"):
        df_rank = pd.read_parquet("latest_snapshot.parquet")
        df_rank["名稱"] = df_rank["股號"].map(stock_map)
        # 固定顯示小數點 2 位
        st.dataframe(df_rank[["股號", "名稱", "大戶%", "大戶週增減", "人數變動", "集中度(大+中)"]].style.format({
            "大戶%": "{:.2f}", "大戶週增減": "{:+.2f}", "人數變動": "{:+.2f}", "集中度(大+中)": "{:.2f}"
        }).map(lambda x: "color: red" if isinstance(x, (int, float)) and x > 0 else "color: green" if isinstance(x, (int, float)) and x < 0 else "", 
        subset=["大戶週增減", "人數變動"]), use_container_width=True, height=500)
    else:
        # --- 診斷區區塊 ---
        all_files = glob.glob("data/chip/**/*.parquet", recursive=True)
        if not all_files:
            st.error("❌ 尚未偵測到任何資料夾，請確認 main.py 是否執行成功。")
        else:
            # 隨機挑一檔檢查日期數量[cite: 12]
            sample_df = pd.read_parquet(all_files[0])
            dates_count = len(sample_df["資料日期"].unique()) if "資料日期" in sample_df.columns else 0
            if dates_count < 2:
                st.warning(f"💡 排行榜尚未產出：目前資料庫僅有 {dates_count} 週資料。")
                st.info("祥哥提示：籌碼分析需要『變動』，請等待下週五集保數據更新後，系統會自動產出排行榜。")
            else:
                st.info("⌛ 排行榜檔案產生中，請稍後刷新網頁。")

# ═══════════════════════════════════════════════════════════════
# Tab 2：個股深度分析 (手機 & 視覺優化[cite: 12])
# ═══════════════════════════════════════════════════════════════
with tab2:
    st.header(f"📈 {sid} {sname} 深度戰情")
    path = Path("data/chip") / sid[:2] / f"{sid}.parquet"
    if not path.exists() or len(d_range) != 2:
        st.warning("等待資料或日期選取中...")
        st.stop()

    start_dt, end_dt = pd.to_datetime(d_range[0]), pd.to_datetime(d_range[1])
    df_chip = load_stock_data(str(path))
    df_chip = df_chip[(df_chip["資料日期"] >= start_dt) & (df_chip["資料日期"] <= end_dt)]
    if df_chip.empty: st.stop()
    df_price = get_price_data(sid, start_dt, end_dt)

    weekly_rows = []
    for d, sub in df_chip.groupby("資料日期"):
        p_match = df_price[df_price.index <= d]
        p_close, p_vol = (float(p_match.iloc[-1]["Close"]), float(p_match.iloc[-1]["Volume"])) if not p_match.empty else (0.0, 0.0)
        bw = float(sub[sub["持股分級"].isin(big_lv)]["權重"].sum())
        mw = float(sub[sub["持股分級"].isin(mid_lv)]["權重"].sum())
        tp = float(sub["人數"].sum())
        weekly_rows.append({"日期": d, "股價": round(p_close, 2), "成交量": int(p_vol), "大戶%": round(bw, 2), "中間戶%": round(mw, 2), "總人數": int(tp)})

    res = pd.DataFrame(weekly_rows).sort_values("日期", ascending=True).reset_index(drop=True)
    res["大戶增減"], res["人數增減"] = res["大戶%"].diff().fillna(0), res["總人數"].diff().fillna(0)

    # ── 圖表調整：手機友和、柱子加粗 ──
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.5, 0.15, 0.35], specs=[[{"secondary_y": True}], [{"secondary_y": False}], [{"secondary_y": False}]])
    
    # 柱子加粗：width 設為 5 天的毫秒數[cite: 12]
    fig.add_trace(go.Bar(x=res["日期"], y=res["總人數"], name="人數", marker_color="royalblue", opacity=0.7, width=432000000), row=3, col=1)
    
    fig.add_trace(go.Scatter(x=res["日期"], y=res["大戶%"], name="大戶%", line=dict(color="red", width=3)), row=1, col=1, secondary_y=True)
    fig.add_trace(go.Scatter(x=res["日期"], y=res["中間戶%"], name="中間戶%", line=dict(color="orange", width=2, dash="dot")), row=1, col=1, secondary_y=True)
    
    if not df_price.empty:
        p_plot = df_price.copy()
        if price_freq == "週資料 (同步)":
            p_plot = p_plot.resample("W-FRI").agg({"Open":"first","High":"max","Low":"min","Close":"last","Volume":"sum"}).dropna()
        fig.add_trace(go.Candlestick(x=p_plot.index, open=p_plot["Open"], high=p_plot["High"], low=p_plot["Low"], close=p_plot["Close"], name="K線"), row=1, col=1)
        fig.add_trace(go.Bar(x=p_plot.index, y=p_plot["Volume"], name="量", marker_color="gray", opacity=0.3), row=2, col=1)

    fig.update_layout(height=650, margin=dict(l=10, r=10, t=30, b=10), template="plotly_dark", hovermode="x unified", xaxis_rangeslider_visible=False, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    st.plotly_chart(fig, use_container_width=True)

    # ── 表格精簡小數點 ──
    st.subheader("📋 詳細指標")
    st.dataframe(res.sort_values("日期", ascending=False).style.format({
        "大戶%": "{:.2f}", "中間戶%": "{:.2f}", "大戶增減": "{:+.2f}", "總人數": "{:,.0f}"
    }).map(lambda x: "color: red" if isinstance(x, (int, float)) and x > 0 else "color: green" if isinstance(x, (int, float)) and x < 0 else "", subset=["大戶增減", "人數增減"]), use_container_width=True)

    # ── 祥哥量化報告文字還原[cite: 12] ──
    st.divider()
    if len(res) >= 2:
        f, l = res.iloc[0], res.iloc[-1]
        c1, c2, c3 = st.columns(3)
        c1.metric("大戶持股變動", f"{l['大戶%']:.2f}%", f"{l['大戶%']-f['大戶%']:+.2f}%")
        c2.metric("總人數增減", f"{l['總人數']:,} 人", f"{l['總人數']-f['總人數']:+.0f} 人", delta_color="inverse")
        if l['大戶%'] > f['大戶%'] and l['總人數'] < f['總人數']: st.success("✅ 【強力吸籌】籌碼極度集中")
    else:
        st.info("💡 區間報告需至少兩週資料。")
