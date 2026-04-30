import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import os
import yfinance as yf
from datetime import datetime, timedelta

# =========================
# 1. 基礎設定與快取讀取
# =========================
st.set_page_config(page_title="祥哥籌碼價量戰情室", layout="wide")

@st.cache_data
def load_stock_map():
    if os.path.exists("stock_map.json"):
        with open("stock_map.json", "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

@st.cache_data
def load_stock_data(path):
    df = pd.read_parquet(path)
    df["資料日期"] = pd.to_datetime(df["資料日期"].astype(str), format='%Y%m%d', errors='coerce')
    df["權重"] = pd.to_numeric(df["權重"], errors="coerce").fillna(0)
    df["人數"] = pd.to_numeric(df["人數"], errors="coerce").fillna(0)
    df["股數"] = pd.to_numeric(df["股數"], errors="coerce").fillna(0)
    return df.dropna(subset=["資料日期"])

@st.cache_data
def get_price_data(sid, start_date, end_date):
    # 基金 (如 00982A) 與一般股票代號處理
    if sid.endswith('A') or sid.endswith('B'):
        tickers = [f"{sid}.TW"]
    else:
        tickers = [f"{sid}.TW", f"{sid}.TWO"]
    
    df = pd.DataFrame()
    for t in tickers:
        df = yf.download(t, start=start_date - timedelta(days=30), end=end_date + timedelta(days=1), progress=False)
        if not df.empty: break
            
    if not df.empty and isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

stock_map = load_stock_map()

# =========================
# 2. Sidebar：核心控制與搜尋
# =========================
with st.sidebar:
    st.header("⚙️ 核心設定")
    
    # 支援代號與中文名稱搜尋
    stock_options = [f"{k} {v}" for k, v in stock_map.items()]
    default_idx = stock_options.index("2330 台積電") if "2330 台積電" in stock_options else 0
    selected_stock = st.selectbox("搜尋股號或名稱", options=stock_options, index=default_idx)
    
    sid = selected_stock.split(" ")[0]
    sname = stock_map.get(sid, "")
    
    today = datetime.now()
    d_range = st.date_input("選擇區間 (量化分析範圍)", [today - timedelta(days=120), today])
    price_freq = st.radio("價量資料頻率", ["日資料", "週資料 (同步籌碼)"], index=0)
    
    st.divider()
    st.subheader("👥 籌碼分級 (1-15級)")
    big_lv = st.multiselect("🔴 大戶定義", options=list(range(1, 16)), default=[15], key="big")
    mid_lv = st.multiselect("🟡 中間戶定義", options=list(range(1, 16)), default=[11, 12, 13, 14], key="mid")
    small_lv = st.multiselect("🟢 散戶定義", options=list(range(1, 16)), default=[1, 2, 3, 4, 5, 6, 7], key="small")
    
    st.divider()
    st.caption("🚀 Powered by 祥哥籌碼模型 v3.5")

# =========================
# 3. 主頁面 Tabs
# =========================
st.title(f"🚀 祥哥籌碼價量戰情室")

tab1, tab2 = st.tabs(["📊 全市場掃描總覽", "🔍 個股深度分析"])

# --- Tab 1: 全市場總覽 (對齊 scanner.py) ---
with tab1:
    st.subheader("🏆 全市場籌碼集中度即時排行榜")
    snapshot_path = "latest_snapshot.parquet"
    
    if os.path.exists(snapshot_path):
        df_rank = pd.read_parquet(snapshot_path)
        
        # 欄位對齊：將 scanner.py 的輸出轉為介面名稱
        rename_map = {
            "代號": "股號", 
            "1000張變動": "大戶週增減", 
            "最新1000張%": "大戶%", 
            "人數變動%": "人數變動"
        }
        df_rank = df_rank.rename(columns=rename_map)

        # 指標卡區域
        c1, c2, c3 = st.columns(3)
        total_stocks = len(df_rank)
        concentrated = len(df_rank[(df_rank["大戶週增減"] > 0) & (df_rank["人數變動"] < 0)])
        
        c1.metric("監控總股數", f"{total_stocks} 檔")
        c2.metric("籌碼集中標的", f"{concentrated} 檔", delta=f"{concentrated/total_stocks:.1%}")
        c3.metric("市場氣氛", "🔥 火熱" if concentrated/total_stocks > 0.15 else "☁ 觀望")

        st.dataframe(
            df_rank[["股號", "名稱", "大戶%", "大戶週增減", "400張變動", "人數變動"]].sort_values("大戶週增減", ascending=False).style.map(
                lambda x: 'color: red' if isinstance(x, (int, float)) and x > 0 else 'color: green' if isinstance(x, (int, float)) and x < 0 else '',
                subset=["大戶週增減", "400張變動", "人數變動"]
            ), use_container_width=True, height=600
        )
    else:
        st.warning("⚠️ 尚未偵測到快照資料。請執行 `python scanner.py` 產出全市場排名。")

# --- Tab 2: 個股深度分析 ---
with tab2:
    st.header(f"📈 {sid} {sname} 深度戰情看板")
    
    folder = f"data/chip/{sid[:2]}"
    path = f"{folder}/{sid}.parquet"

    if os.path.exists(path) and len(d_range) == 2:
        start_dt, end_dt = pd.to_datetime(d_range[0]), pd.to_datetime(d_range[1])
        raw_chip = load_stock_data(path)
        df_chip = raw_chip[(raw_chip["資料日期"] >= start_dt) & (raw_chip["資料日期"] <= end_dt)]
        
        if df_chip.empty:
            st.warning("此區間尚無籌碼資料")
        else:
            df_price = get_price_data(sid, start_dt, end_dt)
            
            # 指標彙整
            weekly_rows = []
            for d, sub in df_chip.groupby("資料日期"):
                p_close = 0.0; p_vol = 0.0
                if not df_price.empty:
                    price_match = df_price[df_price.index <= d]
                    if not price_match.empty:
                        p_close = float(price_match['Close'].iloc[-1]); p_vol = float(price_match['Volume'].iloc[-1])

                def agg_lv(lvs):
                    m = sub["持股分級"].isin(lvs)
                    return sub.loc[m, "權重"].sum(), sub.loc[m, "人數"].sum(), sub.loc[m, "股數"].sum()

                b_w, b_p, b_s = agg_lv(big_lv); m_w, m_p, m_s = agg_lv(mid_lv); s_w, s_p, s_s = agg_lv(small_lv)
                total_p = sub["人數"].sum(); total_s = sub["股數"].sum()

                weekly_rows.append({
                    "日期": d, "股價": round(p_close, 2), "成交量": int(p_vol) if not pd.isna(p_vol) else 0,
                    "大戶%": round(b_w, 2), "中間戶%": round(m_w, 2), "散戶%": round(s_w, 2),
                    "總人數": int(total_p), "人均張數": round((total_s / total_p) / 1000, 2) if total_p > 0 else 0,
                    "集中度": round(b_w + m_w, 2)
                })

            res = pd.DataFrame(weekly_rows).sort_values("日期", ascending=False)
            res["大戶增減"] = res["大戶%"].diff(-1).fillna(0)
            res["人數增減"] = res["總人數"].diff(-1).fillna(0)

            # --- 同步圖表 ---
            fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.05, row_heights=[0.5, 0.2, 0.3], specs=[[{"secondary_y": True}], [{"secondary_y": False}], [{"secondary_y": False}]])
            plot_p = df_price.copy()
            if price_freq == "週資料 (同步籌碼)":
                plot_p = df_price.resample('W-FRI').agg({'Open':'first', 'High':'max', 'Low':'min', 'Close':'last', 'Volume':'sum'}).dropna()
            
            if not plot_p.empty:
                fig.add_trace(go.Candlestick(x=plot_p.index, open=plot_p['Open'], high=plot_p['High'], low=plot_p['Low'], close=plot_p['Close'], name="K線"), row=1, col=1)
                fig.add_trace(go.Bar(x=plot_p.index, y=plot_p['Volume'], name="成交量", marker_color=['red' if c >= o else 'green' for o, c in zip(plot_p['Open'], plot_p['Close'])], opacity=0.5), row=2, col=1)
            
            fig.add_trace(go.Scatter(x=res["日期"], y=res["大戶%"], name="大戶%", line=dict(color='red', width=3)), row=1, col=1, secondary_y=True)
            fig.add_trace(go.Scatter(x=res["日期"], y=res["中間戶%"], name="中間戶%", line=dict(color='orange', width=2, dash='dot')), row=1, col=1, secondary_y=True)
            fig.add_trace(go.Scatter(x=res["日期"], y=res["散戶%"], name="散戶%", line=dict(color='green', width=2)), row=1, col=1, secondary_y=True)
            fig.add_trace(go.Bar(x=res["日期"], y=res["總人數"], name="總人數", marker_color='royalblue', opacity=0.8), row=3, col=1)

            fig.update_layout(height=900, hovermode="x unified", xaxis_rangeslider_visible=False)
            fig.update_yaxes(title_text="人數", row=3, col=1, tickformat=",.0f")
            st.plotly_chart(fig, use_container_width=True)

            # --- 詳細指標看板 ---
            st.subheader("📋 詳細指標看板")
            res_view = res.copy()
            res_view["日期"] = res_view["日期"].dt.strftime('%Y-%m-%d')
            st.dataframe(res_view.style.map(lambda x: 'color: red' if isinstance(x, (int, float)) and x > 0 else 'color: green' if isinstance(x, (int, float)) and x < 0 else '', subset=["大戶增減", "人數增減"]), use_container_width=True)

            # --- 祥哥區間量化報告 ---
            st.divider()
            st.subheader(f"📊 {sid} 祥哥區間量化報告")
            if len(res) >= 2:
                first, last = res.iloc[-1], res.iloc[0]
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.write("**💰 區間總變動**")
                    st.metric("大戶比例淨變動", f"{last['大戶%']:.2f}%", f"{last['大戶%']-first['大戶%']:.2f}%")
                    st.metric("總人數增減", f"{last['總人數']:,} 人", f"{last['總人數']-first['總人數']:.0f} 人", delta_color="inverse")
                with c2:
                    st.write("**📈 慣性與相關性**")
                    conc_count = ((res["大戶增減"] > 0) & (res["人數增減"] < 0)).sum()
                    st.info(f"區間集中週數: **{conc_count}** / {len(res)-1} 週")
                    corr = res[['大戶%', '股價']].corr().iloc[0,1]
                    st.write(f"大戶/股價相關係數: **{corr:.2f}**")
                with c3:
                    st.write("**📝 綜合判斷**")
                    if last['大戶%'] > first['大戶%'] and last['總人數'] < first['總人數']: st.success("✅ 【強力吸籌】籌碼高度集中")
                    elif last['大戶%'] < first['大戶%']: st.error("⚠️ 【籌碼渙散】注意主力撤出")
                    else: st.warning("⚪ 【換手整理】多空勢力拉鋸中")
