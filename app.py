import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import os
import yfinance as yf
from datetime import datetime, timedelta

# =========================
# 1. 核心讀取
# =========================
st.set_page_config(page_title="祥哥籌碼價量戰情室", layout="wide")


# 修正：加上 ttl=3600，Action 跑完後最多一小時內自動刷新，不用手動重啟
@st.cache_data(ttl=3600)
def load_base_data():
    s_map = json.load(open("stock_map.json", "r", encoding="utf-8")) if os.path.exists("stock_map.json") else {}
    rank = pd.read_parquet("latest_snapshot.parquet") if os.path.exists("latest_snapshot.parquet") else pd.DataFrame()
    return s_map, rank


stock_map, df_rank = load_base_data()

# =========================
# 2. Sidebar
# =========================
with st.sidebar:
    st.header("⚙️ 核心設定")
    stock_options = [f"{k} {v}" for k, v in stock_map.items()]
    selected = st.selectbox("搜尋標的", options=stock_options if stock_options else ["無資料"])
    sid = selected.split(" ")[0]
    today = datetime.now()
    d_range = st.date_input("選擇區間", [today - timedelta(days=120), today])

    st.divider()
    st.subheader("👥 級別定義")
    big_lv = st.multiselect("🔴 大戶", range(1, 16), default=[15], key="big")
    mid_lv = st.multiselect("🟡 中間戶", range(1, 16), default=[11, 12, 13, 14], key="mid")
    small_lv = st.multiselect("🟢 散戶", range(1, 16), default=[1, 2, 3, 4, 5, 6, 7], key="small")
    st.caption("Powered by 祥哥籌碼模型 v4.1")

st.title("🚀 祥哥籌碼價量戰情室")
tab1, tab2 = st.tabs(["📊 全市場掃描總覽", "🔍 個股深度分析"])

# --- Tab 1 ---
with tab1:
    st.subheader("🏆 全市場籌碼集中度即時排行榜")
    if not df_rank.empty:
        df_rank["名稱"] = df_rank["股號"].map(stock_map)

        def color_rank(val):
            if isinstance(val, (int, float)):
                if val > 0:
                    return 'color: red'
                elif val < 0:
                    return 'color: green'
            return ''

        st.dataframe(
            df_rank[["股號", "名稱", "大戶%", "大戶週增減", "人數變動", "集中度(大+中)"]
                    ].style.map(color_rank, subset=["大戶週增減", "人數變動"]),
            use_container_width=True
        )
    else:
        st.warning("⚠️ 尚未偵測到排行榜快照，請確認 Action 執行成功並產出 latest_snapshot.parquet")

# --- Tab 2 ---
with tab2:
    st.header(f"📈 {selected} 深度分析報告")
    path = f"data/chip/{sid[:2]}/{sid}.parquet"

    if os.path.exists(path) and len(d_range) == 2:
        df_chip = pd.read_parquet(path)
        df_chip["資料日期"] = pd.to_datetime(df_chip["資料日期"].astype(str), errors='coerce')
        df_chip = df_chip[
            (df_chip["資料日期"] >= pd.to_datetime(d_range[0])) &
            (df_chip["資料日期"] <= pd.to_datetime(d_range[1]))
        ]

        if not df_chip.empty:
            ticker = f"{sid}.TW"

            # 修正：統一處理 yfinance MultiIndex，避免 KeyError
            try:
                price_raw = yf.download(
                    ticker,
                    start=pd.to_datetime(d_range[0]) - timedelta(days=30),
                    progress=False,
                    auto_adjust=True
                )
                if not price_raw.empty:
                    if isinstance(price_raw.columns, pd.MultiIndex):
                        price = price_raw.xs("Close", axis=1, level=0)
                        price = price.to_frame(name="Close") if isinstance(price, pd.Series) else price[["Close"]]
                    else:
                        price = price_raw[["Close"]] if "Close" in price_raw.columns else pd.DataFrame()
                else:
                    price = pd.DataFrame()
            except Exception as e:
                st.warning(f"⚠️ 股價下載失敗: {e}")
                price = pd.DataFrame()

            weekly_rows = []
            for d, sub in df_chip.groupby("資料日期"):
                # 取對應日期股價
                if not price.empty:
                    price_before = price.loc[price.index <= d, "Close"]
                    p_close = float(price_before.iloc[-1]) if not price_before.empty else 0
                else:
                    p_close = 0

                def agg(lvs):
                    m = sub["持股分級"].isin(lvs)
                    w = sub.loc[m, "權重"].sum() if "權重" in sub.columns else 0
                    p = sub.loc[m, "人數"].sum() if "人數" in sub.columns else 0
                    s = sub.loc[m, "股數"].sum() if "股數" in sub.columns else 0
                    return w, p, s

                bw, bp, bs = agg(big_lv)
                mw, mp, ms = agg(mid_lv)
                sw, sp, ss = agg(small_lv)
                tp = sub["人數"].sum() if "人數" in sub.columns else 0
                ts = sub["股數"].sum() if "股數" in sub.columns else 0

                weekly_rows.append({
                    "日期": d,
                    "股價": round(p_close, 2),
                    "大戶%": round(float(bw), 2),
                    "中間戶%": round(float(mw), 2),
                    "散戶%": round(float(sw), 2),
                    "總人數": int(tp),
                    "人均張數": round((float(ts) / float(tp)) / 1000, 2) if tp > 0 else 0
                })

            res = pd.DataFrame(weekly_rows).sort_values("日期", ascending=False)
            # 反向差分（ascending=False 排序後，diff(-1) = 現在 - 更舊的一期，方向正確）
            res["大戶增減"] = res["大戶%"].diff(-1).fillna(0)
            res["人數增減"] = res["總人數"].diff(-1).fillna(0)

            fig = make_subplots(
                rows=2, cols=1,
                shared_xaxes=True,
                row_heights=[0.7, 0.3],
                specs=[[{"secondary_y": True}], [{"secondary_y": False}]]
            )
            fig.add_trace(go.Scatter(x=res["日期"], y=res["股價"], name="股價",
                                     line=dict(color='white', width=1.5), opacity=0.6),
                          row=1, col=1, secondary_y=False)
            fig.add_trace(go.Scatter(x=res["日期"], y=res["大戶%"], name="大戶%",
                                     line=dict(color='red', width=3)),
                          row=1, col=1, secondary_y=True)
            fig.add_trace(go.Scatter(x=res["日期"], y=res["中間戶%"], name="中間戶%",
                                     line=dict(color='orange', dash='dot')),
                          row=1, col=1, secondary_y=True)
            fig.add_trace(go.Bar(x=res["日期"], y=res["總人數"], name="總人數",
                                 marker_color='royalblue', opacity=0.7),
                          row=2, col=1)
            fig.update_layout(height=800, template="plotly_dark", xaxis_rangeslider_visible=False)
            st.plotly_chart(fig, use_container_width=True)

            st.subheader("📊 祥哥區間量化報告")
            if len(res) >= 2:
                f_row, l_row = res.iloc[-1], res.iloc[0]
                c1, c2 = st.columns(2)
                c1.metric(
                    "大戶持股變動",
                    f"{l_row['大戶%']:.2f}%",
                    f"{l_row['大戶%'] - f_row['大戶%']:.2f}%"
                )
                c2.metric(
                    "總人數增減",
                    f"{int(l_row['總人數']):,} 人",
                    f"{int(l_row['總人數'] - f_row['總人數'])} 人",
                    delta_color="inverse"
                )
                if l_row['大戶%'] > f_row['大戶%'] and l_row['總人數'] < f_row['總人數']:
                    st.success("✅ 【強力吸籌】籌碼極度集中")
                elif l_row['大戶%'] < f_row['大戶%'] and l_row['總人數'] > f_row['總人數']:
                    st.warning("⚠️ 【籌碼鬆動】大戶持續出貨")
            else:
                st.info("資料期數不足，無法計算區間變動")

            st.subheader("📋 原始數據")
            st.dataframe(res, use_container_width=True)

        else:
            st.warning("⚠️ 所選區間內無資料，請調整日期範圍")
    else:
        st.warning("⚠️ 數據讀取中，或請確認 data/chip/ 下是否有對應檔案。")
