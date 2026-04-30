import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json, os, yfinance as yf
from datetime import datetime, timedelta

st.set_page_config(page_title="祥哥戰情室", layout="wide")


@st.cache_data
def load_map():
    if os.path.exists("stock_map.json"):
        return json.load(open("stock_map.json", "r", encoding="utf-8"))
    return {}


@st.cache_data
def load_data(path):
    df = pd.read_parquet(path)
    df["資料日期"] = pd.to_datetime(df["資料日期"], errors="coerce")
    return df


stock_map = load_map()

st.sidebar.header("設定")
stock_options = [f"{k} {v}" for k, v in stock_map.items()]
selected = st.sidebar.selectbox("股票", stock_options)
sid = selected.split(" ")[0]

st.title(f"📊 {sid} 戰情室")

path = f"data/chip/{sid[:2]}/{sid}.parquet"

if os.path.exists(path):

    df = load_data(path)

    # 防 NaN
    df = df.dropna(subset=["資料日期"])

    # price
    ticker = f"{sid}.TW"
    price = yf.download(ticker, period="6mo", progress=False)

    if not price.empty:
        if isinstance(price.columns, pd.MultiIndex):
            price.columns = price.columns.get_level_values(0)

        price = price.dropna(subset=["Close"])
        price.index = pd.to_datetime(price.index)

    st.write("籌碼資料")
    st.dataframe(df.tail(20))

    if not price.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=price.index, y=price["Close"], name="Price"))
        st.plotly_chart(fig)

else:
    st.warning("無資料")
