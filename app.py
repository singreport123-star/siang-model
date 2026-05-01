"""
祥哥籌碼價量戰情室 v4.5 - 單位校正版
1. 修正：將「人數變動」與「總人數」單位統一校正為 (千人)。
2. 保持：v4.4 所有功能、格式化邏輯、手機優化與診斷定義。
"""

# ... (前段 import 與讀取 logic 與 v4.4 完全相同，不佔空間略過) ...

# ═══════════════════════════════════════════════════════════════
# Tab 1：全市場排行榜 (修正單位為千人)
# ═══════════════════════════════════════════════════════════════
with tab1:
    st.subheader("🏆 全市場籌碼集中度排行")
    if os.path.exists("latest_snapshot.parquet"):
        df_rank = pd.read_parquet("latest_snapshot.parquet")
        df_rank["名稱"] = df_rank["股號"].map(stock_map)
        
        # 修正：將人數單位改為 (千人)
        df_rank = df_rank.rename(columns={
            "大戶%": "大戶 (%)",
            "大戶週增減": "大戶週增減 (%)",
            "人數變動": "人數變動 (千人)",  # <--- 修正處
            "集中度(大+中)": "集中度 (大+中) (%)"
        })
        
        format_dict = {
            "大戶 (%)": "{:.2f}",
            "大戶週增減 (%)": "{:+.2f}",
            "人數變動 (千人)": "{:+.2f}", # <--- 修正處
            "集中度 (大+中) (%)": "{:.2f}"
        }
        
        st.dataframe(df_rank[["股號", "名稱", "大戶 (%)", "大戶週增減 (%)", "人數變動 (千人)", "集中度 (大+中) (%)"]].style.format(format_dict).map(
            lambda x: "color: red" if isinstance(x, (int, float)) and x > 0 else "color: green" if isinstance(x, (int, float)) and x < 0 else "", 
            subset=["大戶週增減 (%)", "人數變動 (千人)"]), use_container_width=True, height=500)
    else:
        st.info("⌛ 排行榜檔案生成中，請稍後刷新。")

# ═══════════════════════════════════════════════════════════════
# Tab 2：個股深度分析 (同步修正看板單位)
# ═══════════════════════════════════════════════════════════════
with tab2:
    # ... (中間繪圖與邏輯與 v4.4 相同) ...

    st.subheader("📋 區間量化詳細指標看板")
    view = res.sort_values("日期", ascending=False).copy()
    view["日期"] = view["日期"].dt.strftime("%Y-%m-%d")
    
    # 修正看板名稱，增加 (千人)
    view = view.rename(columns={"總人數": "總人數 (千人)", "人數增減": "人數變動 (千人)"})
    
    # 這裡要除以 1000 轉為千人單位顯示
    view["總人數 (千人)"] = view["總人數 (千人)"] / 1000
    view["人數變動 (千人)"] = view["人數變動 (千人)"] / 1000

    res_num_format = {
        "大戶%": "{:.2f}", "中間戶%": "{:.2f}", "散戶%": "{:.2f}", 
        "大戶增減": "{:+.2f}", "散戶增減": "{:+.2f}", 
        "總人數 (千人)": "{:.2f}", "人數變動 (千人)": "{:+.2f}", "人均張數": "{:.2f}"
    }
    
    st.dataframe(view.style.format(res_num_format).map(
        lambda x: "color: red" if isinstance(x, (int, float)) and x > 0 else "color: green" if isinstance(x, (int, float)) and x < 0 else "", 
        subset=["大戶增減", "散戶增減", "人數變動 (千人)"]), use_container_width=True)

    # ... (下方量化報告與說明文檔與 v4.4 相同，metric 部分會自動處理單位) ...
