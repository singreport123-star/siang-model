"""
祥哥籌碼價量戰情室 v3.1
功能完整保留，修正：diff 方向、診斷邏輯、MultiIndex 處理、型別安全
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf
from plotly.subplots import make_subplots

# ═══════════════════════════════════════════════════════════════
# 1. 基礎設定
# ═══════════════════════════════════════════════════════════════
st.set_page_config(page_title="祥哥籌碼價量戰情室", layout="wide")


@st.cache_data
def load_stock_map() -> dict[str, str]:
    path = Path("stock_map.json")
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


@st.cache_data
def load_stock_data(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df["資料日期"] = pd.to_datetime(
        df["資料日期"].astype(str), format="%Y%m%d", errors="coerce"
    )
    for col in ["權重", "人數", "股數"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df.dropna(subset=["資料日期"])


@st.cache_data(ttl=3600)
def get_price_data(
    sid: str, start_date: datetime, end_date: datetime
) -> pd.DataFrame:
    """
    下載股價資料。
    - 一般股票先試 .TW（上市），失敗再試 .TWO（上櫃）
    - 特殊代號（基金 A/B 結尾）直接用 .TW
    修正：正確扁平化 MultiIndex columns
    """
    if sid.endswith(("A", "B")):
        tickers = [f"{sid}.TW"]
    else:
        tickers = [f"{sid}.TW", f"{sid}.TWO"]

    fetch_start = start_date - timedelta(days=30)
    fetch_end   = end_date   + timedelta(days=1)

    for ticker in tickers:
        try:
            df = yf.download(
                ticker,
                start=fetch_start,
                end=fetch_end,
                progress=False,
                auto_adjust=True,
            )
            if df.empty:
                continue
            # 修正：yfinance ≥ 0.2 回傳 MultiIndex columns
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            return df
        except Exception:  # noqa: BLE001
            continue

    return pd.DataFrame()


stock_map = load_stock_map()

# ═══════════════════════════════════════════════════════════════
# 2. Sidebar
# ═══════════════════════════════════════════════════════════════
with st.sidebar:
    st.header("⚙️ 核心設定")

    stock_options = [f"{k} {v}" for k, v in stock_map.items()]
    default_idx = (
        stock_options.index("2330 台積電")
        if "2330 台積電" in stock_options
        else 0
    )
    selected_stock = st.selectbox(
        "搜尋股號或名稱", options=stock_options, index=default_idx
    )
    sid   = selected_stock.split(" ")[0]
    sname = stock_map.get(sid, "")

    today   = datetime.now()
    d_range = st.date_input(
        "選擇區間 (決定量化報告範圍)",
        [today - timedelta(days=120), today],
    )
    price_freq = st.radio(
        "價量資料頻率", ["日資料", "週資料 (同步籌碼)"], index=0
    )

    st.divider()
    st.subheader("👥 級別定義 (1-15級)")
    big_lv   = st.multiselect("🔴 大戶",   options=list(range(1, 16)), default=[15],                        key="big")
    mid_lv   = st.multiselect("🟡 中間戶", options=list(range(1, 16)), default=[11, 12, 13, 14],            key="mid")
    small_lv = st.multiselect("🟢 散戶",   options=list(range(1, 16)), default=list(range(1, 8)),           key="small")

    st.caption("Powered by 祥哥籌碼模型 v3.1")

# ═══════════════════════════════════════════════════════════════
# 3. 頁面標題
# ═══════════════════════════════════════════════════════════════
st.title("🚀 祥哥籌碼價量戰情室 (15級全功能版)")

tab1, tab2 = st.tabs(["📊 全市場掃描總覽", "🔍 個股深度分析"])


# ═══════════════════════════════════════════════════════════════
# Tab 1：全市場總覽
# ═══════════════════════════════════════════════════════════════
with tab1:
    st.subheader("🏆 全市場籌碼集中度即時排行榜")

    if os.path.exists("latest_snapshot.parquet"):
        df_rank = pd.read_parquet("latest_snapshot.parquet")
        df_rank["名稱"] = df_rank["股號"].map(stock_map)

        def _color_num(val):
            if isinstance(val, (int, float)):
                if val > 0:
                    return "color: red"
                if val < 0:
                    return "color: green"
            return ""

        display_cols = ["股號", "名稱", "大戶%", "大戶週增減", "人數變動", "集中度(大+中)"]
        st.dataframe(
            df_rank[display_cols]
            .style.map(_color_num, subset=["大戶週增減", "人數變動"]),
            use_container_width=True,
        )
    else:
        st.info("尚未偵測到全市場快照資料，請先執行 scanner.py。")


# ═══════════════════════════════════════════════════════════════
# Tab 2：個股深度分析
# ═══════════════════════════════════════════════════════════════
with tab2:
    st.header(f"📈 {sid} {sname} 深度戰情看板")

    folder = Path("data/chip") / sid[:2]
    path   = folder / f"{sid}.parquet"

    if not path.exists():
        st.warning(f"找不到 {sid} 的籌碼資料，請確認 data/chip/ 目錄。")
        st.stop()

    if len(d_range) != 2:
        st.warning("請選擇完整的起訖日期。")
        st.stop()

    start_dt, end_dt = pd.to_datetime(d_range[0]), pd.to_datetime(d_range[1])
    raw_chip = load_stock_data(str(path))
    df_chip  = raw_chip[
        (raw_chip["資料日期"] >= start_dt) & (raw_chip["資料日期"] <= end_dt)
    ]

    if df_chip.empty:
        st.warning("此區間尚無籌碼資料。")
        st.stop()

    df_price = get_price_data(sid, start_dt, end_dt)

    # ── 籌碼指標彙整 ─────────────────────────────────────────
    def _agg_levels(sub: pd.DataFrame, levels: list[int]) -> tuple[float, float, float]:
        mask = sub["持股分級"].isin(levels)
        return (
            float(sub.loc[mask, "權重"].sum()),
            float(sub.loc[mask, "人數"].sum()),
            float(sub.loc[mask, "股數"].sum()),
        )

    def _latest_price(df_p: pd.DataFrame, date) -> tuple[float, float]:
        if df_p.empty:
            return 0.0, 0.0
        match = df_p[df_p.index <= date]
        if match.empty:
            return 0.0, 0.0
        row = match.iloc[-1]
        return float(row.get("Close", 0) or 0), float(row.get("Volume", 0) or 0)

    weekly_rows = []
    for d, sub in df_chip.groupby("資料日期"):
        p_close, p_vol = _latest_price(df_price, d)
        b_w, b_p, b_s  = _agg_levels(sub, big_lv)
        m_w, m_p, m_s  = _agg_levels(sub, mid_lv)
        s_w, s_p, s_s  = _agg_levels(sub, small_lv)
        total_p = float(sub["人數"].sum())
        total_s = float(sub["股數"].sum())

        weekly_rows.append({
            "日期":    d,
            "股價":    round(p_close, 2),
            "成交量":  int(p_vol) if not pd.isna(p_vol) else 0,
            "大戶%":   round(b_w, 2),
            "中間戶%": round(m_w, 2),
            "散戶%":   round(s_w, 2),
            "總人數":  int(total_p),
            "人均張數": round((total_s / total_p) / 1000, 2) if total_p > 0 else 0,
            "集中度":  round(b_w + m_w, 2),
        })

    # 修正：按日期升序排列後再計算 diff，增減方向才正確
    res = pd.DataFrame(weekly_rows).sort_values("日期", ascending=True).reset_index(drop=True)
    res["大戶增減"] = res["大戶%"].diff().fillna(0)
    res["散戶增減"] = res["散戶%"].diff().fillna(0)
    res["人數增減"] = res["總人數"].diff().fillna(0)

    # 診斷標籤（修正：人數減少代表籌碼集中，配合正確 diff 方向）
    def _get_diag(row: pd.Series) -> str:
        if row["大戶增減"] > 0 and row["人數增減"] < 0:
            return "🔴 強力吸籌"
        if row["大戶增減"] > 0:
            return "🟡 主力加碼"
        if row["大戶增減"] < 0:
            return "🟠 主力減碼"
        return "⚪ 中性觀望"

    res["診斷"] = res.apply(_get_diag, axis=1)

    # 最終顯示用：降序（最新在前）
    res_desc = res.sort_values("日期", ascending=False).reset_index(drop=True)

    # ── 圖表 ──────────────────────────────────────────────────
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.5, 0.2, 0.3],
        specs=[[{"secondary_y": True}], [{"secondary_y": False}], [{"secondary_y": False}]],
    )

    plot_p = df_price.copy()
    if price_freq == "週資料 (同步籌碼)" and not plot_p.empty:
        plot_p = (
            plot_p.resample("W-FRI")
            .agg({"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"})
            .dropna(subset=["Close"])
        )

    if not plot_p.empty:
        fig.add_trace(
            go.Candlestick(
                x=plot_p.index,
                open=plot_p["Open"], high=plot_p["High"],
                low=plot_p["Low"],   close=plot_p["Close"],
                name="K線",
            ),
            row=1, col=1,
        )
        bar_colors = [
            "red" if c >= o else "green"
            for o, c in zip(plot_p["Open"], plot_p["Close"])
        ]
        fig.add_trace(
            go.Bar(
                x=plot_p.index, y=plot_p["Volume"],
                name="成交量", marker_color=bar_colors, opacity=0.5,
            ),
            row=2, col=1,
        )

    fig.add_trace(
        go.Scatter(x=res["日期"], y=res["大戶%"],   name="大戶%",   line=dict(color="red",    width=3)),
        row=1, col=1, secondary_y=True,
    )
    fig.add_trace(
        go.Scatter(x=res["日期"], y=res["中間戶%"], name="中間戶%", line=dict(color="orange",  width=2, dash="dot")),
        row=1, col=1, secondary_y=True,
    )
    fig.add_trace(
        go.Scatter(x=res["日期"], y=res["散戶%"],   name="散戶%",   line=dict(color="green",   width=2)),
        row=1, col=1, secondary_y=True,
    )
    fig.add_trace(
        go.Bar(x=res["日期"], y=res["總人數"], name="總人數", marker_color="royalblue", opacity=0.8),
        row=3, col=1,
    )

    fig.update_layout(
        height=900,
        hovermode="x unified",
        xaxis_rangeslider_visible=False,
    )
    fig.update_yaxes(title_text="人數", row=3, col=1, tickformat=",.0f")
    st.plotly_chart(fig, use_container_width=True)

    # ── 指標看板 ──────────────────────────────────────────────
    st.subheader("📋 區間量化詳細指標看板")
    view = res_desc.copy()
    view["日期"] = view["日期"].dt.strftime("%Y-%m-%d")
    st.dataframe(
        view.style.map(
            lambda x: "color: red" if isinstance(x, (int, float)) and x > 0
                      else "color: green" if isinstance(x, (int, float)) and x < 0
                      else "",
            subset=["大戶增減", "散戶增減", "人數增減"],
        ),
        use_container_width=True,
    )

    # ── 祥哥區間量化報告 ─────────────────────────────────────
    st.divider()
    st.subheader(f"📊 {sid} 祥哥區間量化報告")

    if len(res) >= 2:
        first_row = res.iloc[0]   # 升序：第一筆 = 最舊
        last_row  = res.iloc[-1]  # 升序：最後一筆 = 最新

        c1, c2, c3 = st.columns(3)

        with c1:
            st.write("**💰 區間總變動**")
            big_delta = last_row["大戶%"] - first_row["大戶%"]
            ppl_delta = last_row["總人數"] - first_row["總人數"]
            st.metric("大戶持股變動", f"{last_row['大戶%']:.2f}%",  f"{big_delta:+.2f}%")
            st.metric(
                "總人數增減",
                f"{last_row['總人數']:,} 人",
                f"{ppl_delta:+.0f} 人",
                delta_color="inverse",
            )

        with c2:
            st.write("**📉 持續性分析**")
            conc_count = ((res["大戶增減"] > 0) & (res["人數增減"] < 0)).sum()
            st.info(f"區間集中慣性：**{conc_count}** / {len(res) - 1} 週")
            if len(res[["大戶%", "股價"]].dropna()) >= 2:
                corr = res[["大戶%", "股價"]].corr().iloc[0, 1]
                st.write(f"大戶/股價相關性：**{corr:.2f}**")
            else:
                st.write("大戶/股價相關性：資料不足")

        with c3:
            st.write("**📝 綜合判斷**")
            if big_delta > 0 and ppl_delta < 0:
                st.success("✅ 【強力吸籌】籌碼極度集中")
            elif big_delta < 0:
                st.error("⚠️ 【籌碼渙散】注意主力撤出")
            else:
                st.warning("⚪ 【盤整換手】多空力道拉鋸")
    else:
        st.info("💡 區間報告需至少兩週資料。")
