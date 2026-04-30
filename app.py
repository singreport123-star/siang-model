import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import os
import yfinance as yf
from datetime import datetime, timedelta

# =========================
# 1. 基礎設定
# =========================
st.set_page_config(page_title="祥哥籌碼價量戰情室", layout="wide")
st.title("🚀 祥哥籌碼價量戰情室 (15級全功能版)")

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
    ticker = f"{sid}.TW" if len(sid) == 4 else f"{sid}.TWO"
    df = yf.download(ticker, start=start_date - timedelta(days=30), end=end_date + timedelta(days=1), progress=False)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

stock_map = load_stock_map()

# =========================
# 2. Sidebar 控制區
# =========================
with st.sidebar:
    st.header("⚙️ 核心設定")
    sid = st.text_input("輸入股號", value="2330").strip()
    
    today = datetime.now()
    # 這裡確保日期選擇器能抓到區間
    d_range = st.date_input("選擇日期區間", [today - timedelta(days=120), today])
    
    price_freq = st.radio("價量資料頻率", ["日資料", "週資料 (同步籌碼)"], index=0)
    
    st.divider()
    st.subheader("👥 籌碼級別定義")
    big_lv = st.multiselect("🔴 大戶", options=list(range(1, 16)), default=[15], key="big")
    mid_lv = st.multiselect("🟡 中間戶", options=list(range(1, 16)), default=[11, 12, 13, 14], key="mid")
    small_lv = st.multiselect("🟢 散戶", options=list(range(1, 16)), default=[1, 2, 3, 4, 5, 6, 7], key="small")

# =========================
# 3. 資料處理與指標運算
# =========================
folder = f"data/chip/{sid[:2]}"
path = f"{folder}/{sid}.parquet"

if os.path.exists(path) and len(d_range) == 2:
    start_dt, end_dt = pd.to_datetime(d_range[0]), pd.to_datetime(d_range[1])
    raw_chip = load_stock_data(path)
    # 過濾選擇的日期區間
    df_chip = raw_chip[(raw_chip["資料日期"] >= start_dt) & (raw_chip["資料日期"] <= end_dt)]
    
    if df_chip.empty:
        st.warning("此區間尚無籌碼資料，請嘗試調整日期區間。")
    else:
        df_price = get_price_data(sid, start_dt, end_dt)
        
        weekly_rows = []
        for d, sub in df_chip.groupby("資料日期"):
            p_close = 0.0; p_vol = 0.0
            if not df_price.empty:
                price_match = df_price[df_price.index <= d]
                if not price_match.empty:
                    p_close = float(price_match['Close'].iloc[-1])
                    p_vol = float(price_match['Volume'].iloc[-1])

            def agg_lv(lvs):
                m = sub["持股分級"].isin(lvs)
                return sub.loc[m, "權重"].sum(), sub.loc[m, "人數"].sum(), sub.loc[m, "股數"].sum()

            b_w, b_p, b_s = agg_lv(big_lv)
            m_w, m_p, m_s = agg_lv(mid_lv)
            s_w, s_p, s_s = agg_lv(small_lv)
            total_p = sub["人數"].sum()
            total_s = sub["股數"].sum()

            weekly_rows.append({
                "日期": d, "股價": round(p_close, 2), "成交量": int(p_vol) if not pd.isna(p_vol) else 0,
                "大戶%": round(b_w, 2), "中間戶%": round(m_w, 2), "散戶%": round(s_w, 2),
                "總人數": int(total_p), "人均張數": round((total_s / total_p) / 1000, 2) if total_p > 0 else 0,
                "集中度": round(b_w + m_w, 2)
            })

        res = pd.DataFrame(weekly_rows).sort_values("日期", ascending=False)
        # 計算週增減
        res["大戶增減"] = res["大戶%"].diff(-1).fillna(0)
        res["散戶增減"] = res["散戶%"].diff(-1).fillna(0)
        res["人數增減"] = res["總人數"].diff(-1).fillna(0)

        def get_diagnosis(row):
            if row["大戶增減"] > 0 and row["人數增減"] < 0: return "🔴 強力吸籌"
            if row["大戶增減"] > 0: return "🟡 主力加碼"
            if row["散戶增減"] > 0.5: return "🟢 籌碼渙散"
            return "⚪ 中性觀望"
        res["診斷"] = res.apply(get_diagnosis, axis=1)

        # =========================
        # 4. 圖表區
        # =========================
        fig = make_subplots(
            rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.05,
            row_heights=[0.5, 0.2, 0.3],
            subplot_titles=("價量籌碼對照圖 (紅:大戶, 橘:中間, 綠:散戶)", "成交量", "股東人數走勢"),
            specs=[[{"secondary_y": True}], [{"secondary_y": False}], [{"secondary_y": False}]]
        )

        plot_p = df_price.copy()
        if price_freq == "週資料 (同步籌碼)":
            plot_p = df_price.resample('W-FRI').agg({'Open':'first', 'High':'max', 'Low':'min', 'Close':'last', 'Volume':'sum'}).dropna()
        
        fig.add_trace(go.Candlestick(x=plot_p.index, open=plot_p['Open'], high=plot_p['High'], low=plot_p['Low'], close=plot_p['Close'], name="K線"), row=1, col=1)
        fig.add_trace(go.Scatter(x=res["日期"], y=res["大戶%"], name="大戶%", line=dict(color='red', width=3)), row=1, col=1, secondary_y=True)
        fig.add_trace(go.Scatter(x=res["日期"], y=res["中間戶%"], name="中間戶%", line=dict(color='orange', width=2, dash='dot')), row=1, col=1, secondary_y=True)
        fig.add_trace(go.Scatter(x=res["日期"], y=res["散戶%"], name="散戶%", line=dict(color='green', width=2)), row=1, col=1, secondary_y=True)

        v_colors = ['red' if c >= o else 'green' for o, c in zip(plot_p['Open'], plot_p['Close'])]
        fig.add_trace(go.Bar(x=plot_p.index, y=plot_p['Volume'], name="成交量", marker_color=v_colors, opacity=0.5), row=2, col=1)
        fig.add_trace(go.Bar(x=res["日期"], y=res["總人數"], name="總人數", marker_color='royalblue', opacity=0.8), row=3, col=1)

        fig.update_layout(height=900, hovermode="x unified", xaxis_rangeslider_visible=False)
        fig.update_yaxes(title_text="人數", row=3, col=1, tickformat=",.0f")
        fig.update_xaxes(type='date', rangebreaks=[dict(bounds=["sat", "mon"])])
        st.plotly_chart(fig, use_container_width=True)

        # =========================
        # 5. 指標看板 (修正日期與診斷呈現)
        # =========================
        st.subheader("📋 區間量化詳細指標看板")
        # 格式化日期為字串，避免表格出現 00:00:00
        res_display = res.copy()
        res_display["日期"] = res_display["日期"].dt.strftime('%Y-%m-%d')
        
        view_cols = ["日期", "股價", "成交量", "大戶%", "大戶增減", "中間戶%", "散戶%", "散戶增減", "總人數", "人數增減", "人均張數", "集中度", "診斷"]
        
        styled_res = res_display[view_cols].style.format({
            "股價": "{:.2f}", "成交量": "{:,}", "大戶%": "{:.2f}%", "大戶增減": "{:+.2f}%",
            "中間戶%": "{:.2f}%", "散戶%": "{:.2f}%", "散戶增減": "{:+.2f}%", 
            "總人數": "{:,}", "人數增減": "{:+,.0f}", "人均張數": "{:.2f}", "集中度": "{:.2f}%"
        })
        
        def color_logic(val):
            if isinstance(val, (int, float)):
                if val > 0: return 'color: red'
                if val < 0: return 'color: green'
            return ''

        st.dataframe(styled_res.map(color_logic, subset=["大戶增減", "散戶增減", "人數增減"]), use_container_width=True, height=400)

        # =========================
        # 6. 祥哥區間量化報告 (這部分我補齊了)
        # =========================
        st.divider()
        st.subheader("📊 祥哥區間量化報告")
        
        if len(res) >= 2:
            first, last = res.iloc[-1], res.iloc[0]
            
            c1, c2, c3 = st.columns(3)
            with c1:
                st.write("**💰 區間變動彙整**")
                st.metric("大戶比例淨變動", f"{last['大戶%']:.2f}%", f"{last['大戶%']-first['大戶%']:.2f}%")
                st.metric("散戶比例淨變動", f"{last['散戶%']:.2f}%", f"{last['散戶%']-first['散戶%']:.2f}%", delta_color="inverse")
                st.metric("總人數增減", f"{last['總人數']:,} 人", f"{last['總人數']-first['總人數']:.0f} 人", delta_color="inverse")
            
            with c2:
                st.write("**📈 統計與相關性**")
                # 集中週數：大戶增 + 人數減
                conc_weeks = ((res["大戶增減"] > 0) & (res["人數增減"] < 0)).sum()
                total_weeks = len(res) - 1
                ratio = (conc_weeks / total_weeks * 100) if total_weeks > 0 else 0
                st.info(f"區間共 {total_weeks} 週，有 **{conc_weeks} 週** 籌碼集中 (佔 {ratio:.0f}%)")
                
                # 價格相關性
                corr = res[['大戶%', '股價']].corr().iloc[0,1]
                st.write(f"大戶持股比與股價相關性：**{corr:.2f}**")
                if corr > 0.7: st.success("🔥 籌碼對股價具有高度導向力")
                
            with c3:
                st.write("**📝 祥哥綜合判斷**")
                if last['大戶%'] > first['大戶%'] and last['總人數'] < first['總人數']:
                    st.success("✅ 區間判斷：【主力強力吸籌】")
                    st.write("籌碼從散戶流向大戶，具備波段發動潛力。")
                elif last['大戶%'] < first['大戶%'] and last['總人數'] > first['總人數']:
                    st.error("⚠️ 區間判斷：【籌碼散亂出貨】")
                    st.write("散戶接盤，主力退場，需注意價格修正風險。")
                else:
                    st.warning("⚪ 區間判斷：【盤整換手區】")
                    st.write("籌碼變動方向不一，建議觀察支撐位。")
