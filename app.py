"""
祥哥籌碼價量戰情室 v3.6 - 終極完全體
功能：完整還原 v3.1 所有指標[cite: 9, 12] + v3.3 診斷邏輯 + 手機滑動優化
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
# 1. 基礎設定與資料讀取[cite: 9, 12]
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
    tickers = [f"{sid}.TW", f"{sid}.TWO"] if not sid.endswith(("A", "B")) else [f"{sid}.TW"]
    for t in tickers:
        try:
            df = yf.download(t, start=start_date-timedelta(days=30), end=end_date+timedelta(days=1), progress=False, auto_adjust=True)
            if not df.empty:
                if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
                return df
        except: continue
    return pd.DataFrame()

stock_map = load_stock_map()

# ═══════════════════════════════════════════════════════════════
# 2. Sidebar (手機版樣式適配[cite: 12])
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
    st.subheader("👥 級別定義 (1-15級)")
    big_lv   = st.multiselect("🔴 大戶",   options=list(range(1, 16)), default=[15], key="big")
    mid_lv   = st.multiselect("🟡 中間戶", options=list(range(1, 16)), default=[11, 12, 13, 14], key="mid")
    small_lv = st.multiselect("🟢 散戶",   options=list(range(1, 16)), default=list(range(1, 8)), key="small")
    st.caption("Powered by 祥哥籌碼模型 v3.6")

st.title("🚀 祥哥籌碼價量戰情室 (完全體)")
tab1, tab2 = st.tabs(["📊 市場排行榜", "🔍 個股深度分析"])

# ═══════════════════════════════════════════════════════════════
# Tab 1：全市場排行榜 (診斷邏輯補完[cite: 12])
# ═══════════════════════════════════════════════════════════════
with tab1:
    st.subheader("🏆 全市場籌碼集中度排行")
    if os.path.exists("latest_snapshot.parquet"):
        df_rank = pd.read_parquet("latest_snapshot.parquet")
        df_rank["名稱"] = df_rank["股號"].map(stock_map)
        st.dataframe(df_rank[["股號", "名稱", "大戶%", "大戶週增減", "人數變動", "集中度(大+中)"]].style.format("{:.2f}").map(
            lambda x: "color: red" if isinstance(x, (int, float)) and x > 0 else "color: green" if isinstance(x, (int, float)) and x < 0 else "", 
            subset=["大戶週增減", "人數變動"]), use_container_width=True, height=500)
    else:
        # --- 提示詞與診斷區 ---
        all_files = glob.glob("data/chip/**/*.parquet", recursive=True)
        if not all_files:
            st.error("❌ 尚未偵測到資料庫，請確認 Action 是否正確下載資料。")
        else:
            sample_df = pd.read_parquet(all_files[0])
            dates_count = len(sample_df["資料日期"].unique()) if "資料日期" in sample_df.columns else 0
            if dates_count < 2:
                st.warning(f"💡 排行榜需至少兩週資料，目前資料庫僅有 {dates_count} 週。")
                st.info("祥哥提示：籌碼分析需觀察『變動』，請等待下週更新後系統自動產出排行。")
            else:
                st.info("⌛ 排行榜檔案生成中，請稍後刷新。")

# ═══════════════════════════════════════════════════════════════
# Tab 2：個股深度分析 (功能 100% 還原[cite: 9, 12])
# ═══════════════════════════════════════════════════════════════
with tab2:
    st.header(f"📈 {sid} {sname} 戰情看板")
    path = Path("data/chip") / sid[:2] / f"{sid}.parquet"
    if not path.exists() or len(d_range) != 2:
        st.warning("⚠️ 找不到籌碼資料或日期未選全。")
        st.stop()

    start_dt, end_dt = pd.to_datetime(d_range[0]), pd.to_datetime(d_range[1])
    raw_chip = load_stock_data(str(path))
    df_chip = raw_chip[(raw_chip["資料日期"] >= start_dt) & (raw_chip["資料日期"] <= end_dt)]
    if df_chip.empty: st.stop()
    df_price = get_price_data(sid, start_dt, end_dt)

    # 籌碼指標彙整[cite: 9, 12]
    weekly_rows = []
    for d, sub in df_chip.groupby("資料日期"):
        pm = df_price[df_price.index <= d]
        p_close, p_vol = (float(pm.iloc[-1]["Close"]), float(pm.iloc[-1]["Volume"] / 1000)) if not pm.empty else (0.0, 0.0)
        
        def _agg(lvs):
            m = sub["持股分級"].isin(lvs)
            return float(sub.loc[m, "權重"].sum()), float(sub.loc[m, "人數"].sum()), float(sub.loc[m, "股數"].sum())
        
        bw, bp, bs = _agg(big_lv); mw, mp, ms = _agg(mid_lv); sw, sp, ss = _agg(small_lv)
        tp, ts = float(sub["人數"].sum()), float(sub["股數"].sum())
        
        weekly_rows.append({
            "日期": d, "股價": round(p_close, 2), "成交張數": int(p_vol),
            "大戶%": round(bw, 2), "中間戶%": round(mw, 2), "散戶%": round(sw, 2),
            "總人數": int(tp), "人均張數": round((ts/tp)/1000, 2) if tp > 0 else 0,
            "集中度": round(bw + mw, 2)
        })

    res = pd.DataFrame(weekly_rows).sort_values("日期", ascending=True).reset_index(drop=True)
    res["大戶增減"], res["散戶增減"], res["人數增減"] = res["大戶%"].diff().fillna(0), res["散戶%"].diff().fillna(0), res["總人數"].diff().fillna(0)
    
    # 診斷標籤[cite: 9, 12]
    def _diag_row(row):
        if row["大戶增減"] > 0 and row["人數增減"] < 0: return "🔴 強力吸籌"
        if row["大戶增減"] > 0: return "🟡 主力加碼"
        if row["大戶增減"] < 0: return "🟠 主力減碼"
        return "⚪ 中性觀望"
    res["診斷"] = res.apply(_diag_row, axis=1)

    # ── 圖表調整：禁用拖曳 (RWD) 與 柱子加粗[cite: 12] ──
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.5, 0.15, 0.35], specs=[[{"secondary_y": True}], [{"secondary_y": False}], [{"secondary_y": False}]])
    
    # 柱子加粗
    fig.add_trace(go.Bar(x=res["日期"], y=res["總人數"], name="總人數", marker_color="royalblue", opacity=0.8, width=432000000), row=3, col=1)
    
    fig.add_trace(go.Scatter(x=res["日期"], y=res["大戶%"], name="大戶%", line=dict(color="red", width=3)), row=1, col=1, secondary_y=True)
    fig.add_trace(go.Scatter(x=res["日期"], y=res["中間戶%"], name="中間戶%", line=dict(color="orange", width=2, dash="dot")), row=1, col=1, secondary_y=True)
    fig.add_trace(go.Scatter(x=res["日期"], y=res["散戶%"], name="散戶%", line=dict(color="green", width=2)), row=1, col=1, secondary_y=True)

    if not df_price.empty:
        p_plot = df_price.copy()
        if price_freq == "週資料 (同步)":
            p_plot = p_plot.resample("W-FRI").agg({"Open":"first","High":"max","Low":"min","Close":"last","Volume":"sum"}).dropna()
        fig.add_trace(go.Candlestick(x=p_plot.index, open=p_plot["Open"], high=p_plot["High"], low=p_plot["Low"], close=p_plot["Close"], name="K線"), row=1, col=1)
        # 成交量彩色化[cite: 12]
        v_colors = ['red' if c >= o else 'green' for o, c in zip(p_plot['Open'], p_plot['Close'])]
        fig.add_trace(go.Bar(x=p_plot.index, y=p_plot["Volume"] / 1000, name="成交張數", marker_color=v_colors, opacity=0.8), row=2, col=1)

    # 禁用縮放與拖曳 (手機版順滑關鍵[cite: 12])
    fig.update_layout(height=650, margin=dict(l=10, r=10, t=30, b=10), dragmode=False, hovermode="x unified", xaxis_rangeslider_visible=False, template="plotly_dark", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    st.plotly_chart(fig, use_container_width=True, config={'staticPlot': False, 'scrollZoom': False, 'displayModeBar': False})

    # ── 指標看板：格式精確化[cite: 9, 12] ──
    st.subheader("📋 區間量化詳細指標看板")
    view = res.sort_values("日期", ascending=False).copy()
    view["日期"] = view["日期"].dt.strftime("%Y-%m-%d")
    st.dataframe(view.style.format({"大戶%": "{:.2f}", "中間戶%": "{:.2f}", "散戶%": "{:.2f}", "大戶增減": "{:+.2f}", "散戶增減": "{:+.2f}", "總人數": "{:,}", "人均張數": "{:.2f}"}).map(lambda x: "color: red" if isinstance(x, (int, float)) and x > 0 else "color: green" if isinstance(x, (int, float)) and x < 0 else "", subset=["大戶增減", "散戶增減", "人數增減"]), use_container_width=True)

    # ── 祥哥量化報告 (文字 100% 還原[cite: 9, 12]) ──
    st.divider()
    st.subheader(f"📊 {sid} 祥哥區間量化報告")
    if len(res) >= 2:
        f, l = res.iloc[0], res.iloc[-1]
        c1, c2, c3 = st.columns(3)
        with c1:
            st.write("**💰 區間總變動**")
            st.metric("大戶持股變動", f"{l['大戶%']:.2f}%", f"{l['大戶%']-f['大戶%']:+.2f}%")
            st.metric("總人數增減", f"{l['總人數']:,} 人", f"{l['總人數']-f['總人數']:+.0f} 人", delta_color="inverse")
            st.metric("人均張數變動", f"{l['人均張數']:.2f} 張", f"{l['人均張數']-f['人均張數']:+.2f} 張")
        with c2:
            st.write("**📉 持續性分析**")
            conc_count = ((res["大戶增減"] > 0) & (res["人數增減"] < 0)).sum()
            st.info(f"區間集中慣性：**{conc_count}** / {len(res)-1} 週")
            if len(res[["大戶%", "股價"]].dropna()) >= 2:
                st.write(f"大戶/股價相關性：**{res[['大戶%', '股價']].corr().iloc[0, 1]:.2f}**")
        with c3:
            st.write("**📝 綜合判斷**")
            big_delta, ppl_delta = (l["大戶%"] - f["大戶%"]), (l["總人數"] - f["總人數"])
            if big_delta > 0 and ppl_delta < 0: st.success("✅ 【強力吸籌】籌碼極度集中")
            elif big_delta < 0: st.error("⚠️ 【籌碼渙散】注意主力撤出")
            else: st.warning("⚪ 【盤整換手】多空力道拉鋸")
            st.write(f"最新一期診斷：**{res['診斷'].iloc[-1]}**")
    else:
        st.info("💡 區間報告需至少兩週資料。")
