import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import os
import yfinance as yf
from datetime import datetime, timedelta

# =========================
# 1. 基礎設定與資料讀取
# =========================
st.set_page_config(page_title="祥哥籌碼價量戰情室", layout="wide")

@st.cache_data(ttl=3600)
def load_stock_map():
    if os.path.exists("stock_map.json"):
        with open("stock_map.json", "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

@st.cache_data(ttl=3600)
def load_snapshot():
    if os.path.exists("latest_snapshot.parquet"):
        return pd.read_parquet("latest_snapshot.parquet")
    return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_stock_data(path):
    df = pd.read_parquet(path)
    # 相容 main.py 修正版：資料日期可能是 %Y%m%d 純數字，也可能已是標準格式
    df["資料日期"] = pd.to_datetime(df["資料日期"].astype(str), errors='coerce')
    df["權重"] = pd.to_numeric(df["權重"], errors="coerce").fillna(0)
    df["人數"] = pd.to_numeric(df["人數"], errors="coerce").fillna(0)
    # 股數欄位在 main.py 修正版中已統一命名為「股數」
    if "股數" in df.columns:
        df["股數"] = pd.to_numeric(df["股數"], errors="coerce").fillna(0)
    else:
        df["股數"] = 0
    # 持股分級確認
    if "持股分級" not in df.columns:
        st.error("❌ 此檔案缺少「持股分級」欄位，請重新執行 Action 更新資料")
        return pd.DataFrame()
    return df.dropna(subset=["資料日期"])

@st.cache_data(ttl=3600)
def get_price_data(sid, start_date, end_date):
    # 基金(如 00982A/B) 與一般股票的代號備案處理
    if sid.endswith('A') or sid.endswith('B'):
        tickers = [f"{sid}.TW"]
    else:
        tickers = [f"{sid}.TW", f"{sid}.TWO"]

    for t in tickers:
        try:
            raw = yf.download(
                t,
                start=start_date - timedelta(days=30),
                end=end_date + timedelta(days=1),
                progress=False,
                auto_adjust=True
            )
            if raw.empty:
                continue

            # 修正：yfinance 新版單一股票也會回傳 MultiIndex(Field, Ticker)
            # droplevel(1) 移除 Ticker 層，只留 Open/High/Low/Close/Volume
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.droplevel(1)

            if "Close" not in raw.columns:
                continue

            return raw[["Open", "High", "Low", "Close", "Volume"]]

        except Exception as e:
            st.warning(f"⚠️ {t} 下載失敗: {e}")
            continue

    return pd.DataFrame()

stock_map = load_stock_map()
df_rank = load_snapshot()

# =========================
# 2. Sidebar：祥哥特製聰明搜尋
# =========================
with st.sidebar:
    st.header("⚙️ 核心設定")
    stock_options = [f"{k} {v}" for k, v in stock_map.items()]

    if not stock_options:
        st.warning("⚠️ stock_map.json 尚未產生，請先執行 Action")
        stock_options = ["請先執行 Action"]

    default_idx = 0
    if "2330 台積電" in stock_options:
        default_idx = stock_options.index("2330 台積電")

    selected_stock = st.selectbox("搜尋股號或名稱", options=stock_options, index=default_idx)
    sid = selected_stock.split(" ")[0]
    sname = stock_map.get(sid, "")

    today = datetime.now()
    d_range = st.date_input("選擇區間 (決定量化報告範圍)", [today - timedelta(days=120), today])
    price_freq = st.radio("價量資料頻率", ["日資料", "週資料 (同步籌碼)"], index=0)

    st.divider()
    st.subheader("👥 級別定義 (1-15級)")
    big_lv   = st.multiselect("🔴 大戶",  options=list(range(1, 16)), default=[15],                       key="big")
    mid_lv   = st.multiselect("🟡 中間戶", options=list(range(1, 16)), default=[11, 12, 13, 14],           key="mid")
    small_lv = st.multiselect("🟢 散戶",  options=list(range(1, 16)), default=[1, 2, 3, 4, 5, 6, 7],     key="small")

    st.divider()
    st.caption("Powered by 祥哥籌碼模型 v4.1")
    st.caption(f"資料更新時間：每週五 UTC+8 16:00")

# =========================
# 3. 主頁面
# =========================
st.title("🚀 祥哥籌碼價量戰情室 (15級全功能版)")

tab1, tab2 = st.tabs(["📊 全市場掃描總覽", "🔍 個股深度分析"])

# ─────────────────────────────────────────
# Tab 1：全市場總覽
# ─────────────────────────────────────────
with tab1:
    st.subheader("🏆 全市場籌碼集中度即時排行榜")

    if not df_rank.empty:
        df_show = df_rank.copy()
        df_show["名稱"] = df_show["股號"].map(stock_map)

        # 欄位順序對齊 scanner.py 修正版輸出
        show_cols = [c for c in ["股號", "名稱", "大戶%", "大戶週增減", "人數變動", "集中度(大+中)"] if c in df_show.columns]

        def color_rank(val):
            if isinstance(val, (int, float)):
                return 'color: red' if val > 0 else 'color: green' if val < 0 else ''
            return ''

        st.dataframe(
            df_show[show_cols].style.map(color_rank, subset=["大戶週增減", "人數變動"]),
            use_container_width=True,
            height=600
        )

        # 快速統計
        c1, c2, c3 = st.columns(3)
        c1.metric("掃描標的總數", f"{len(df_show):,} 檔")
        if "大戶週增減" in df_show.columns:
            bull = (df_show["大戶週增減"] > 0).sum()
            bear = (df_show["大戶週增減"] < 0).sum()
            c2.metric("大戶增加標的", f"{bull} 檔", delta=f"+{bull}", delta_color="normal")
            c3.metric("大戶減少標的", f"{bear} 檔", delta=f"-{bear}", delta_color="inverse")
    else:
        st.info("⏳ 尚未偵測到全市場快照資料，請確認 GitHub Action 已成功執行並產出 latest_snapshot.parquet")

# ─────────────────────────────────────────
# Tab 2：個股深度分析
# ─────────────────────────────────────────
with tab2:
    st.header(f"📈 {sid} {sname} 深度戰情看板")

    folder = f"data/chip/{sid[:2]}"
    path   = f"{folder}/{sid}.parquet"

    if not os.path.exists(path):
        st.warning(f"⚠️ 找不到 {path}，請確認 Action 已執行且該標的有資料")
    elif len(d_range) != 2:
        st.warning("⚠️ 請選擇完整的起訖日期")
    else:
        start_dt = pd.to_datetime(d_range[0])
        end_dt   = pd.to_datetime(d_range[1])

        raw_chip = load_stock_data(path)

        if raw_chip.empty:
            st.error("❌ 籌碼資料讀取失敗，請確認資料格式")
        else:
            df_chip = raw_chip[(raw_chip["資料日期"] >= start_dt) & (raw_chip["資料日期"] <= end_dt)]

            if df_chip.empty:
                st.warning("⚠️ 此區間尚無籌碼資料，請調整日期範圍")
            else:
                # 股價資料
                with st.spinner(f"下載 {sid} 股價中..."):
                    df_price = get_price_data(sid, start_dt, end_dt)

                if df_price.empty:
                    st.info(f"ℹ️ 無法取得 {sid} 股價（可能為非上市櫃標的），K線圖將略過")

                # ── 籌碼指標彙整 ──
                weekly_rows = []
                for d, sub in df_chip.groupby("資料日期"):
                    p_close = 0.0
                    p_vol   = 0.0
                    if not df_price.empty:
                        price_match = df_price[df_price.index <= d]
                        if not price_match.empty:
                            p_close = float(price_match['Close'].iloc[-1])
                            p_vol   = float(price_match['Volume'].iloc[-1])

                    def agg_lv(lvs):
                        m = sub["持股分級"].isin(lvs)
                        w = float(sub.loc[m, "權重"].sum())
                        p = float(sub.loc[m, "人數"].sum())
                        s = float(sub.loc[m, "股數"].sum()) if "股數" in sub.columns else 0.0
                        return w, p, s

                    b_w, b_p, b_s = agg_lv(big_lv)
                    m_w, m_p, m_s = agg_lv(mid_lv)
                    s_w, s_p, s_s = agg_lv(small_lv)
                    total_p = float(sub["人數"].sum())
                    total_s = float(sub["股數"].sum()) if "股數" in sub.columns else 0.0

                    weekly_rows.append({
                        "日期":     d,
                        "股價":     round(p_close, 2),
                        "成交量":   int(p_vol) if not pd.isna(p_vol) else 0,
                        "大戶%":    round(b_w, 2),
                        "中間戶%":  round(m_w, 2),
                        "散戶%":    round(s_w, 2),
                        "總人數":   int(total_p),
                        "人均張數": round((total_s / total_p) / 1000, 2) if total_p > 0 else 0,
                        "集中度":   round(b_w + m_w, 2)
                    })

                res = pd.DataFrame(weekly_rows).sort_values("日期", ascending=False)
                # ascending=False 後 diff(-1) = 本期 - 上一期（較舊），方向正確
                res["大戶增減"] = res["大戶%"].diff(-1).fillna(0)
                res["散戶增減"] = res["散戶%"].diff(-1).fillna(0)
                res["人數增減"] = res["總人數"].diff(-1).fillna(0)

                def get_diag(row):
                    if row["大戶增減"] > 0 and row["人數增減"] < 0:
                        return "🔴 強力吸籌"
                    if row["大戶增減"] > 0:
                        return "🟡 主力加碼"
                    return "⚪ 中性觀望"
                res["診斷"] = res.apply(get_diag, axis=1)

                # ── 圖表 ──
                fig = make_subplots(
                    rows=3, cols=1,
                    shared_xaxes=True,
                    vertical_spacing=0.05,
                    row_heights=[0.5, 0.2, 0.3],
                    specs=[[{"secondary_y": True}], [{"secondary_y": False}], [{"secondary_y": False}]]
                )

                plot_p = df_price.copy()
                if price_freq == "週資料 (同步籌碼)" and not plot_p.empty:
                    plot_p = plot_p.resample('W-FRI').agg({
                        'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'
                    }).dropna()

                if not plot_p.empty:
                    fig.add_trace(go.Candlestick(
                        x=plot_p.index,
                        open=plot_p['Open'], high=plot_p['High'],
                        low=plot_p['Low'],   close=plot_p['Close'],
                        name="K線",
                        increasing_line_color='red',
                        decreasing_line_color='green'
                    ), row=1, col=1)

                    vol_colors = ['red' if c >= o else 'green'
                                  for o, c in zip(plot_p['Open'], plot_p['Close'])]
                    fig.add_trace(go.Bar(
                        x=plot_p.index, y=plot_p['Volume'],
                        name="成交量", marker_color=vol_colors, opacity=0.5
                    ), row=2, col=1)

                # 籌碼線
                fig.add_trace(go.Scatter(x=res["日期"], y=res["大戶%"],   name="大戶%",   line=dict(color='red',    width=3)),             row=1, col=1, secondary_y=True)
                fig.add_trace(go.Scatter(x=res["日期"], y=res["中間戶%"], name="中間戶%", line=dict(color='orange', width=2, dash='dot')), row=1, col=1, secondary_y=True)
                fig.add_trace(go.Scatter(x=res["日期"], y=res["散戶%"],   name="散戶%",   line=dict(color='green',  width=2)),             row=1, col=1, secondary_y=True)

                # 人數柱
                fig.add_trace(go.Bar(
                    x=res["日期"], y=res["總人數"],
                    name="總人數", marker_color='royalblue', opacity=0.8
                ), row=3, col=1)

                fig.update_layout(
                    height=900,
                    hovermode="x unified",
                    xaxis_rangeslider_visible=False,
                    template="plotly_dark",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                fig.update_yaxes(title_text="籌碼 %",  row=1, col=1, secondary_y=True)
                fig.update_yaxes(title_text="成交量",  row=2, col=1, tickformat=",.0f")
                fig.update_yaxes(title_text="股東人數", row=3, col=1, tickformat=",.0f")

                st.plotly_chart(fig, use_container_width=True)

                # ── 指標看板 ──
                st.subheader("📋 區間量化詳細指標看板")
                res_view = res.copy()
                res_view["日期"] = res_view["日期"].dt.strftime('%Y-%m-%d')

                num_cols = ["大戶增減", "散戶增減", "人數增減"]
                st.dataframe(
                    res_view.style.map(
                        lambda x: 'color: red' if isinstance(x, (int, float)) and x > 0
                                  else 'color: green' if isinstance(x, (int, float)) and x < 0
                                  else '',
                        subset=num_cols
                    ),
                    use_container_width=True
                )

                # ── 祥哥區間量化報告 ──
                st.divider()
                st.subheader(f"📊 {sid} {sname} 祥哥區間量化報告")

                if len(res) >= 2:
                    first_row = res.iloc[-1]   # 最舊
                    last_row  = res.iloc[0]    # 最新

                    c1, c2, c3 = st.columns(3)

                    with c1:
                        st.write("**💰 區間總變動**")
                        st.metric(
                            "大戶持股變動",
                            f"{last_row['大戶%']:.2f}%",
                            f"{last_row['大戶%'] - first_row['大戶%']:.2f}%"
                        )
                        st.metric(
                            "總人數增減",
                            f"{int(last_row['總人數']):,} 人",
                            f"{int(last_row['總人數'] - first_row['總人數'])} 人",
                            delta_color="inverse"
                        )
                        st.metric(
                            "人均張數變動",
                            f"{last_row['人均張數']:.2f} 張",
                            f"{last_row['人均張數'] - first_row['人均張數']:.2f} 張"
                        )

                    with c2:
                        st.write("**📉 持續性分析**")
                        conc_count = ((res["大戶增減"] > 0) & (res["人數增減"] < 0)).sum()
                        total_weeks = max(len(res) - 1, 1)
                        st.info(f"區間集中慣性：**{conc_count}** / {total_weeks} 週")

                        if last_row['股價'] > 0 and first_row['股價'] > 0:
                            price_chg = (last_row['股價'] - first_row['股價']) / first_row['股價'] * 100
                            st.metric("區間股價漲跌", f"{last_row['股價']:.1f}", f"{price_chg:.1f}%")

                        try:
                            corr_val = res[['大戶%', '股價']].corr().iloc[0, 1]
                            st.write(f"大戶/股價相關性：**{corr_val:.2f}**")
                        except Exception:
                            st.write("大戶/股價相關性：資料不足")

                    with c3:
                        st.write("**📝 綜合判斷**")
                        big_up   = last_row['大戶%'] > first_row['大戶%']
                        ppl_down = last_row['總人數'] < first_row['總人數']

                        if big_up and ppl_down:
                            st.success("✅ 【強力吸籌】籌碼極度集中")
                        elif big_up and not ppl_down:
                            st.warning("🟡 【主力加碼】人數同步增加，注意散戶追高")
                        elif not big_up and ppl_down:
                            st.warning("🟠 【籌碼整理】大戶微減但人數收斂")
                        else:
                            st.error("⚠️ 【籌碼渙散】注意主力撤出風險")

                        st.write("")
                        latest_diag = res["診斷"].iloc[0]
                        st.write(f"最新一期診斷：**{latest_diag}**")

                else:
                    st.info("💡 區間量化報告需至少兩週資料，請拉大日期範圍。")
