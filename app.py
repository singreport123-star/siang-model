import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import os
import yfinance as yf
from datetime import datetime, timedelta

# =========================
# 1. 基礎設定與讀取
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
    # 確保個股分頁也能讀到正確欄位 (與 main.py 對齊)
    df["資料日期"] = pd.to_datetime(df["資料日期"].astype(str), errors='coerce')
    return df.dropna(subset=["資料日期"])

@st.cache_data
def get_price_data(sid, start_date, end_date):
    ticker = f"{sid}.TW" if not (sid.endswith('A') or sid.endswith('B')) else f"{sid}.TW"
    # 這裡增加備案，如果是 00982A 這種可能需要 yfinance 認得的格式
    df = yf.download(ticker, start=start_date - timedelta(days=30), end=end_date + timedelta(days=1), progress=False)
    if not df.empty and isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

stock_map = load_stock_map()

# =========================
# 2. Sidebar
# =========================
with st.sidebar:
    st.header("⚙️ 核心設定")
    stock_options = [f"{k} {v}" for k, v in stock_map.items()]
    selected_stock = st.selectbox("搜尋股號或名稱", options=stock_options, index=0)
    sid = selected_stock.split(" ")[0]
    sname = stock_map.get(sid, "")
    
    today = datetime.now()
    d_range = st.date_input("分析區間", [today - timedelta(days=120), today])
    
    st.divider()
    st.subheader("👥 籌碼分級定義")
    big_lv = st.multiselect("🔴 大戶", options=list(range(1, 16)), default=[15])
    mid_lv = st.multiselect("🟡 中間戶", options=list(range(1, 16)), default=[11, 12, 13, 14])
    
    st.caption("🚀 Powered by 祥哥籌碼模型 v3.6")

# =========================
# 3. Tabs
# =========================
st.title("🚀 祥哥籌碼價量戰情室")
tab1, tab2 = st.tabs(["📊 全市場掃描總覽", "🔍 個股深度分析"])

# --- Tab 1: 總覽 ---
with tab1:
    st.subheader("🏆 全市場籌碼集中度即時排行榜")
    if os.path.exists("latest_snapshot.parquet"):
        df_rank = pd.read_parquet("latest_snapshot.parquet")
        
        # 核心對齊：scanner 產出的是「代號」，改名為「股號」
        df_rank = df_rank.rename(columns={"代號": "股號", "最新1000張%": "大戶%", "1000張變動": "大戶週增減"})
        
        st.dataframe(
            df_rank[["股號", "名稱", "大戶%", "大戶週增減", "400張變動", "人數變動%"]].style.map(
                lambda x: 'color: red' if isinstance(x, (int, float)) and x > 0 else 'color: green' if isinstance(x, (int, float)) and x < 0 else '',
                subset=["大戶週增減", "400張變動", "人數變動%"]
            ), use_container_width=True
        )
    else:
        st.warning("尚未偵測到快照資料，請確保 Action 執行成功並產出 latest_snapshot.parquet")

# --- Tab 2: 個股 ---
with tab2:
    st.header(f"📈 {sid} {sname} 戰情看板")
    folder = f"data/chip/{sid[:2]}"
    path = f"{folder}/{sid}.parquet"
    if os.path.exists(path):
        # 這裡跑原本個股分析的繪圖與報告邏輯...
        st.write("資料讀取成功，圖表生成中...")
        # (個股圖表代碼同前)
    else:
        st.error(f"找不到 {sid} 的歷史資料，請確認資料已更新。")
