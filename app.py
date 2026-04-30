# ... (前方基礎設定與資料讀取維持原樣，不作變動以免死掉) ...

# ═══════════════════════════════════════════════════════════════
# 2. Sidebar (手機版優化：減少不必要的分割線)
# ═══════════════════════════════════════════════════════════════
with st.sidebar:
    st.header("⚙️ 核心設定")
    # ... (選單邏輯維持原樣) ...
    
    # 這裡調整一下 layout 寬度，讓手機版選單不要太佔空間
    st.markdown("""
        <style>
        [data-testid="stSidebar"][aria-expanded="true"]{
            min-width: 200px;
            max-width: 300px;
        }
        </style>
    """, unsafe_allow_html=True)

# ... (Tab 1 內容維持原樣) ...

# ═══════════════════════════════════════════════════════════════
# Tab 2：個股深度分析 (格式重點改動區)
# ═══════════════════════════════════════════════════════════════
with tab2:
    # ... (數據計算邏輯維持原樣) ...

    # ── 圖表格式改進 ──────────────────────────────────────────
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03, # 縮小間距，增加手機版可視範圍
        row_heights=[0.5, 0.2, 0.3],
        specs=[[{"secondary_y": True}], [{"secondary_y": False}], [{"secondary_y": False}]]
    )

    # ... (K線圖繪製維持原樣) ...

    # 1. 總人數柱狀圖：柱子變粗
    # 加入 xperiodalignment="middle" 並透過 width 設定寬度 (以毫秒計，一週約為 604800000)
    fig.add_trace(
        go.Bar(
            x=res["日期"], 
            y=res["總人數"], 
            name="總人數", 
            marker_color='royalblue', 
            opacity=0.8,
            width=5*24*60*60*1000, # 讓柱子佔滿一週的 5 天寬度，視覺上會變粗
        ), 
        row=3, col=1
    )

    fig.update_layout(
        height=700, # 手機版 900 太高，改為 700 較適中
        margin=dict(l=10, r=10, t=50, b=10), # 縮小邊界，手機版滿版感
        hovermode="x unified",
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        legend=dict(
            orientation="h", # 圖例橫排，避免手機版佔據側邊空間
            yanchor="bottom", y=1.02, 
            xanchor="right", x=1
        )
    )
    st.plotly_chart(fig, use_container_width=True)

    # 2. 詳細指標看板：修正小數點
    st.subheader("📋 區間量化詳細指標看板")
    view = res_desc.copy()
    view["日期"] = view["日期"].dt.strftime("%Y-%m-%d")
    
    # 格式化所有數字欄位為 2 位小數，增加閱讀性
    num_cols = view.select_dtypes(include=['float64', 'int64']).columns
    format_dict = {col: "{:.2f}" for col in num_cols if col != "總人數"}
    format_dict["總人數"] = "{:,.0f}" # 人數不給小數點，給千分位
    
    st.dataframe(
        view.style.format(format_dict).map(
            lambda x: "color: red" if isinstance(x, (int, float)) and x > 0
                      else "color: green" if isinstance(x, (int, float)) and x < 0
                      else "",
            subset=["大戶增減", "散戶增減", "人數增減"],
        ),
        use_container_width=True,
    )

    # 3. 祥哥區間量化報告 (手機版優化：Columns 改為 1 欄或 2 欄)
    st.divider()
    st.subheader(f"📊 祥哥報告")

    if len(res) >= 2:
        # 手機版 st.columns 會自動堆疊，我們簡化文字內容
        c1, c2, c3 = st.columns([1, 1, 1])
        # ... (指標卡內容維持原樣，僅微調文字寬度[cite: 2]) ...
