import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import os
import yfinance as yf
from datetime import datetime, timedelta

# =========================
# 1. 基礎設定與快取
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
    # 強制修正日期：防止 1970 Unix Timestamp 錯誤
    df["資料日期"] = pd.to_datetime(df["資料日期"].astype(str), format='%Y%m%d', errors='coerce')
    df["權重"] = pd.to_numeric(df["權重"], errors="coerce").fillna(0)
    df["人數"] = pd.to_numeric(df["人數"], errors="coerce").fillna(0)
    df["股數"] = pd.to_numeric(df["股數"], errors="coerce").fillna(0)
    return df.dropna(subset=["資料日期"])

@st.cache_data
def get_price_data(sid, start_date, end_date):
    # 判斷股號長度
    ticker = f"{sid}.TW" if len(sid) == 4 else f"{sid}.TWO"
    # 多抓 14 天確保均線與週五資料對齊
    df = yf.download(ticker, start=start_date - timedelta(days=14), end=end_date + timedelta(days=1), progress=False)
    # 移除 MultiIndex (yfinance v0.2.x 之後版本可能會有這問題)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

stock_map = load_stock_map()

# =========================
# 2. Sidebar 控制與 15 級勾選
# =========================
with st.sidebar:
    st.header("⚙️ 核心設定")
    sid = st.text_input("輸入股號 (如 2330, 00981A)", value="2330").strip()
    
    # 橫軸日期區間選取器 (月曆)
    today = datetime.now()
    d_range = st.date_input("選擇日期區間 (月曆選取)", [today - timedelta(days=120), today])
    
    st.divider()
    st.subheader("👥 籌碼級別定義 (1-15級)")
    big_lv = st.multiselect("🔴 大戶定義", options=list(range(1, 16)), default=[15], key="big")
    mid_lv = st.multiselect("🟡 中間戶定義", options=list(range(1, 16)), default=[11, 12, 13, 14], key="mid")
    small_lv = st.multiselect("🟢 散戶定義", options=list(range(1, 16)), default=[1, 2, 3, 4, 5, 6, 7], key="small")

# =========================
# 3. 核心運算邏輯
# =========================
folder = f"data/chip/{sid[:2]}"
path = f"{folder}/{sid}.parquet"

if os.path.exists(path) and len(d_range) == 2:
    start_dt, end_dt = pd.to_datetime(d_range[0]), pd.to_datetime(d_range[1])
    
    # 讀取並按日期區間過濾
    raw_chip = load_stock_data(path)
    df_chip = raw_chip[(raw_chip["資料日期"] >= start_dt) & (raw_chip["資料日期"] <= end_dt)]
    
    if df_chip.empty:
        st.warning("此區間尚無籌碼資料，請嘗試拉長日期範圍。")
    else:
        # 抓取股價 (YFinance)
        df_price = get_price_data(sid, start_dt, end_dt)
        
        # 建立週度指標表
        weekly_rows = []
        for d, sub in df_chip.groupby("資料日期"):
            # 對齊當週收盤價與成交量
            p_close = 0.0
            p_vol = 0.0
            if not df_price.empty:
                # 找出小於等於當天日期的最後一筆價格
                price_match = df_price[df_price.index <= d]
                if not price_match.empty:
                    # 使用 .iloc[-1] 取得最後一筆，並用 float() 強制轉換
                    p_close = float(price_match['Close'].iloc[-1])
                    p_vol = float(price_match['Volume'].iloc[-1])

            # 聚合級別數據
            def agg_lv(lvs):
                m = sub["持股分級"].isin(lvs)
                return sub.loc[m, "權重"].sum(), sub.loc[m, "人數"].sum(), sub.loc[m, "股數"].sum()

            b_w, b_p, b_s = agg_lv(big_lv)
            m_w, m_p, m_s = agg_lv(mid_lv)
            s_w, s_p, s_s = agg_lv(small_lv)
            total_p = sub["人數"].sum()
            total_s = sub["股數"].sum()

            weekly_rows.append({
                "日期": d,
                "股價": round(p_close, 2),
                "成交量": int(p_vol) if not pd.isna(p_vol) else 0, # 防呆修正
                "大戶%": round(b_w, 2),
                "中間戶%": round(m_w, 2),
                "散戶%": round(s_w, 2),
                "總人數": int(total_p),
                "人均張數": round((total_s / total_p) / 1000, 2) if total_p > 0 else 0,
                "集中度(大+中)": round(b_w + m_w, 2)
            })

        # 轉成 DataFrame 並計算變動
        res = pd.DataFrame(weekly_rows).sort_values("日期", ascending=False)
        res["大戶增減"] = res["大戶%"].diff(-1).fillna(0)
        res["散戶增減"] = res["散戶%"].diff(-1).fillna(0)
        res["人數增減"] = res["總人數"].diff(-1).fillna(0)

        # 自動診斷邏輯
        def get_diagnosis(row):
            if row["大戶增減"] > 0 and row["人數增減"] < 0: return "🔴 強力吸籌"
            if row["大戶增減"] > 0: return "🟡 主力加碼"
            if row["散戶增減"] > 0.5: return "🟢 籌碼渙散"
            return "⚪ 中性觀望"
        res["診斷"] = res.apply(get_diagnosis, axis=1)

        # =========================
        # 4. 中部：三合一同步圖表
        # =========================
        fig = make_subplots(
            rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.03,
            row_heights=[0.5, 0.2, 0.3],
            subplot_titles=("價量籌碼對照圖", "", "股東人數走勢"),
            specs=[[{"secondary_y": True}], [{"secondary_y": False}], [{"secondary_y": False}]]
        )

        # (1) K線與比例線
        if not df_price.empty:
            fig.add_trace(go.Candlestick(x=df_price.index, open=df_price['Open'], high=df_price['High'], low=df_price['Low'], close=df_price['Close'], name="K線"), row=1, col=1)
        
        fig.add_trace(go.Scatter(x=res["日期"], y=res["大戶%"], name="大戶%", line=dict(color='red', width=3)), row=1, col=1, secondary_y=True)
        fig.add_trace(go.Scatter(x=res["日期"], y=res["散戶%"], name="散戶%", line=dict(color='green', width=2)), row=1, col=1, secondary_y=True)

        # (2) 成交量柱狀圖 (紅漲綠跌)
        if not df_price.empty:
            # 確保 Close 跟 Open 沒有 NaN 才能比較
            valid_price = df_price.dropna(subset=['Open', 'Close'])
            v_colors = ['red' if c >= o else 'green' for o, c in zip(valid_price['Open'], valid_price['Close'])]
            fig.add_trace(go.Bar(x=valid_price.index, y=valid_price['Volume'], name="成交量", marker_color=v_colors, opacity=0.5), row=2, col=1)

        # (3) 總人數線
        fig.add_trace(go.Scatter(x=res["日期"], y=res["總人數"], name="總人數", line=dict(color='royalblue', dash='dot')), row=3, col=1)

        fig.update_layout(height=800, hovermode="x unified", xaxis_rangeslider_visible=False, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        fig.update_xaxes(type='date', rangebreaks=[dict(bounds=["sat", "mon"])])
        st.plotly_chart(fig, use_container_width=True)

        # =========================
        # 5. 下部：詳細量化指標看板
        # =========================
        st.subheader("📋 區間量化詳細指標看板")
        view_cols = ["日期", "股價", "成交量", "大戶%", "大戶增減", "散戶%", "散戶增減", "總人數", "人數增減", "人均張數", "集中度(大+中)", "診斷"]
        
        styled_res = res[view_cols].style.format({
            "股價": "{:.2f}", "成交量": "{:,}", "大戶%": "{:.2f}%", "大戶增減": "{:+.2f}%",
            "散戶%": "{:.2f}%", "散戶增減": "{:+.2f}%", "總人數": "{:,}", "人數增減": "{:+,.0f}",
            "集中度(大+中)": "{:.2f}%"
        })
        
        def color_logic(val):
            if isinstance(val, (int, float)):
                if val > 0: return 'color: red'
                if val < 0: return 'color: green'
            return ''

        st.dataframe(
            styled_res.applymap(color_logic, subset=["大戶增減", "散戶增減", "人數增減"]),
            use_container_width=True, height=500
        )

        # =========================
        # 6. 底部：祥哥量化分析報告
        # =========================
        st.divider()
        c1, c2, c3 = st.columns(3)
        if len(res) >= 2:
            first, last = res.iloc[-1], res.iloc[0]
            with c1:
                st.metric("區間大戶變動", f"{last['大戶%']:.2f}%", f"{last['大戶%']-first['大戶%']:.2f}%")
            with c2:
                st.metric("區間總人數變動", f"{last['總人數']:,} 人", f"{last['總人數']-first['總人數']:.0f} 人", delta_color="inverse")
            with c3:
                conc_count = ((res["大戶增減"] > 0) & (res["人數增減"] < 0)).sum()
                st.write(f"📊 **籌碼診斷：**")
                st.write(f"- 區間集中週數: **{conc_count}** / {len(res)-1} 週")
                st.write(f"- 價格相關性 (大戶/%): **{res[['大戶%', '股價']].corr().iloc[0,1]:.2f}**")

else:
    st.info("請於側邊欄輸入股號並確認日期區間，系統將自動啟動戰情看板。")
