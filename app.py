import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import os

# =========================
# 基本設定
# =========================
st.set_page_config(page_title="祥哥籌碼戰情室", layout="wide")
st.title("🚀 祥哥籌碼戰情室 - 15x3 矩陣掃描")

# =========================
# 快取資料 - 優化網頁速度
# =========================
@st.cache_data
def load_stock_map():
    if os.path.exists("stock_map.json"):
        with open("stock_map.json", "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

@st.cache_data
def load_snapshot():
    return pd.read_parquet("latest_snapshot.parquet")

@st.cache_data
def load_stock_data(path):
    df = pd.read_parquet(path)
    # 確保日期格式正確
    df["資料日期"] = pd.to_datetime(df["資料日期"], errors="coerce")
    return df.dropna(subset=["資料日期"])

stock_map = load_stock_map()

# =========================
# 分頁
# =========================
tab1, tab2 = st.tabs(["📊 全市場掃描排行", "🔍 個股深度分析"])

# =========================
# Tab 1: 全市場排行
# =========================
with tab1:
    st.subheader("主力吸籌排行榜")
    if os.path.exists("latest_snapshot.parquet"):
        df_rank = load_snapshot()
        display_df = df_rank.rename(columns={
            "1000張變動": "1000張變動(%)",
            "400張變動": "400張變動(%)",
            "人數變動%": "股東人數變動(%)",
            "最新1000張%": "最新1000張持股(%)"
        })
        display_df = display_df[[
            "代號", "名稱", "1000張變動(%)", "400張變動(%)", "股東人數變動(%)", "最新1000張持股(%)"
        ]]
        st.dataframe(
            display_df.style
            .background_gradient(subset=["1000張變動(%)", "400張變動(%)"], cmap="Reds")
            .background_gradient(subset=["股東人數變動(%)"], cmap="Greens_r"),
            use_container_width=True, height=600
        )
    else:
        st.warning("尚未偵測到排行榜資料")

# =========================
# Tab 2: 個股分析
# =========================
with tab2:
    col1, col2 = st.columns([1, 4])
    with col1:
        sid = st.text_input("請輸入股號", value="2330").strip()
    
    # 支援帶有字母的代號 (例如 00981A)
    if not sid:
        st.stop()

    folder = f"data/chip/{sid[:2]}"
    path = f"{folder}/{sid}.parquet"

    if os.path.exists(path):
        with st.spinner("讀取資料中..."):
            df = load_stock_data(path)

        st.subheader(f"{sid} {stock_map.get(sid, '')}")

        # --- 向量化計算 (已修正欄位名稱為 權重/人數) ---
        grouped = df.groupby("資料日期")
        res = pd.DataFrame({
            "1000張%": grouped.apply(lambda x: x.loc[x["持股分級"] == 15, "權重"].sum()),
            "400張%": grouped.apply(lambda x: x.loc[x["持股分級"] >= 11, "權重"].sum()),
            "總人數": grouped["人數"].sum()
        }).reset_index().rename(columns={"資料日期": "日期"})

        res = res.sort_values("日期")

        # --- 指標卡 ---
        if len(res) >= 2:
            latest, prev = res.iloc[-1], res.iloc[-2]
            c1, c2, c3 = st.columns(3)
            c1.metric("1000張大戶比例", f"{latest['1000張%']:.2f}%", f"{latest['1000張%']-prev['1000張%']:.2f}%")
            c2.metric("400張以上大戶", f"{latest['400張%']:.2f}%", f"{latest['400張%']-prev['400張%']:.2f}%")
            c3.metric("總股東人數", f"{int(latest['總人數'])}", f"{latest['總人數']-prev['總人數']:.0f}", delta_color="inverse")

        # --- 圖表 ---
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(x=res["日期"], y=res["1000張%"], name="1000張%", marker_color='rgba(255, 99, 132, 0.6)'), secondary_y=False)
        fig.add_trace(go.Scatter(x=res["日期"], y=res["400張%"], name="400張%", line=dict(color='firebrick')), secondary_y=False)
        fig.add_trace(go.Scatter(x=res["日期"], y=res["總人數"], name="總人數", line=dict(color='royalblue')), secondary_y=True)
        fig.update_layout(hovermode="x unified", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("歷史明細")
        st.dataframe(res.sort_values("日期", ascending=False), use_container_width=True)
    else:
        st.error(f"找不到 {sid} 的資料")
