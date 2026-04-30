import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json, os, yfinance as yf
from datetime import datetime, timedelta

st.set_page_config(page_title="祥哥籌碼價量戰情室", layout="wide")

@st.cache_data(ttl=3600)
def load_base():
    s_map = json.load(open("stock_map.json", "r", encoding="utf-8")) if os.path.exists("stock_map.json") else {}
    rank = pd.read_parquet("latest_snapshot.parquet") if os.path.exists("latest_snapshot.parquet") else pd.DataFrame()
    return s_map, rank

@st.cache_data(ttl=3600)
def load_stock_data(path):
    df = pd.read_parquet(path)
    df.columns = [str(c).strip().replace("\u3000", "").replace("%", "") for c in df.columns]
    # 動態映射，防止 KeyError
    cmap = {"持股分級": "持股分級", "人數": "人數", "權重": "權重", "股數": "股數", "資料日期": "資料日期"}
    for c in df.columns:
        if "分級" in c: cmap[c] = "持股分級"
        elif "人數" in c: cmap[c] = "人數"
        elif "權重" in c: cmap[c] = "權重"
        elif "資料日期" in c: cmap[c] = "資料日期"
    df = df.rename(columns=cmap)
    df["資料日期"] = pd.to_datetime(df["資料日期"].astype(str), errors="coerce")
    for c in ["權重", "人數", "股數"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df.dropna(subset=["資料日期"])

stock_map, df_rank = load_base()

with st.sidebar:
    st.header("⚙️ 核心設定")
    stock_options = [f"{k} {v}" for k, v in stock_map.items()]
    selected = st.selectbox("搜尋標的", options=stock_options if stock_options else ["無資料"])
    sid = selected.split(" ")[0]
    d_range = st.date_input("分析區間", [datetime.now() - timedelta(days=120), datetime.now()])
    
    st.divider()
    big_lv = st.multiselect("🔴 大戶", range(1, 16), default=[15], key="big")
    mid_lv = st.multiselect("🟡 中間戶", range(1, 16), default=[11, 12, 13, 14], key="mid")
    small_lv = st.multiselect("🟢 散戶", range(1, 16), default=[1,2,3,4,5,6,7], key="small")
    st.caption("Powered by 祥哥籌碼模型 v4.3")

st.title("🚀 祥哥籌碼價量戰情室")
t1, t2 = st.tabs(["📊 市場總覽", "🔍 個股分析"])

with t1:
    if not df_rank.empty:
        df_rank["名稱"] = df_rank["股號"].map(stock_map)
        st.dataframe(df_rank.style.map(lambda x: 'color: red' if isinstance(x, (int, float)) and x > 0 else 'color: green' if isinstance(x, (int, float)) and x < 0 else '', subset=["大戶週增減", "人數變動"]), use_container_width=True)
    else:
        st.info("⏳ 尚未產出排行榜快照")

with t2:
    path = f"data/chip/{sid[:2]}/{sid}.parquet"
    if os.path.exists(path) and len(d_range) == 2:
        df = load_stock_data(path)
        df = df[(df["資料日期"] >= pd.to_datetime(d_range[0])) & (df["資料日期"] <= pd.to_datetime(d_range[1]))]
        
        # 價格抓取 (修正 yfinance MultiIndex 問題)
        p_raw = yf.download(f"{sid}.TW", start=pd.to_datetime(d_range[0])-timedelta(days=30), progress=False)
        if not p_raw.empty and isinstance(p_raw.columns, pd.MultiIndex): p_raw.columns = p_raw.columns.droplevel(1)
        
        if not df.empty:
            weekly = []
            for d, sub in df.groupby("資料日期"):
                def agg(lvs):
                    m = sub["持股分級"].isin(lvs)
                    return float(sub.loc[m, "權重"].sum()), float(sub.loc[m, "人數"].sum())
                bw, bp = agg(big_lv); mw, mp = agg(mid_lv); sw, sp = agg(small_lv)
                close = float(p_raw.loc[p_raw.index <= d, "Close"].iloc[-1]) if not p_raw.empty and not p_raw.loc[p_raw.index <= d].empty else 0
                weekly.append({"日期": d, "股價": round(close, 2), "大戶%": round(bw, 2), "中間戶%": round(mw, 2), "散戶%": round(sw, 2), "總人數": int(sub["人數"].sum())})
            
            res = pd.DataFrame(weekly).sort_values("日期", ascending=False)
            res["大戶增減"] = res["大戶%"].diff(-1).fillna(0)
            res["人數增減"] = res["總人數"].diff(-1).fillna(0)

            # 圖表
            fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], specs=[[{"secondary_y": True}], [{"secondary_y": False}]])
            fig.add_trace(go.Scatter(x=res["日期"], y=res["大戶%"], name="大戶%", line=dict(color='red', width=3)), secondary_y=True)
            fig.add_trace(go.Scatter(x=res["日期"], y=res["中間戶%"], name="中間戶%", line=dict(color='orange', dash='dot')), secondary_y=True)
            fig.add_trace(go.Bar(x=res["日期"], y=res["總人數"], name="總人數", marker_color='royalblue', opacity=0.7), row=2, col=1)
            fig.update_layout(height=700, template="plotly_dark", xaxis_rangeslider_visible=False)
            st.plotly_chart(fig, use_container_width=True)
            
            # 量化報告
            c1, c2 = st.columns(2)
            f, l = res.iloc[-1], res.iloc[0]
            c1.metric("大戶週增減", f"{l['大戶%']:.2f}%", f"{l['大戶增減']:.2f}%")
            c2.metric("總人數變動", f"{l['總人數']:,} 人", f"{l['人數增減']:.0f} 人", delta_color="inverse")
            if l["大戶增減"] > 0 and l["人數增減"] < 0: st.success("✅ 【強力吸籌】籌碼極度集中")
