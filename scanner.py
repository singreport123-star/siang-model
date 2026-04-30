import pandas as pd
import glob
import os
import json


def calculate_metrics(df):
    if df.empty:
        return None

    # 確認關鍵欄位存在
    for col in ["資料日期", "持股分級", "權重", "人數"]:
        if col not in df.columns:
            print(f"⚠ 缺少欄位 {col}，跳過此檔")
            return None

    df["資料日期"] = pd.to_datetime(df["資料日期"].astype(str), errors="coerce")
    df = df.dropna(subset=["資料日期"]).sort_values("資料日期")
    dates = df["資料日期"].unique()

    if len(dates) < 2:
        return None

    l_df = df[df["資料日期"] == dates[-1]]
    p_df = df[df["資料日期"] == dates[-2]]

    l_1000 = l_df.loc[l_df["持股分級"] == 15, "權重"].sum()
    p_1000 = p_df.loc[p_df["持股分級"] == 15, "權重"].sum()
    l_h = l_df["人數"].sum()
    p_h = p_df["人數"].sum()
    l_conc = l_df.loc[l_df["持股分級"] >= 11, "權重"].sum()

    return {
        "股號": None,
        "大戶%": round(float(l_1000), 2),
        "大戶週增減": round(float(l_1000 - p_1000), 2),
        "人數變動": round(((l_h - p_h) / p_h) * 100, 2) if p_h > 0 else 0,
        "集中度(大+中)": round(float(l_conc), 2)
    }


def process_file(f, stock_map):
    sid = os.path.basename(f).replace(".parquet", "")
    try:
        df = pd.read_parquet(f)
        m = calculate_metrics(df)
        if m:
            m["股號"] = sid
            m["名稱"] = stock_map.get(sid, "未知")
            return m
    except Exception as e:
        print(f"⚠ 處理 {sid} 失敗: {e}")
    return None


def run_scan():
    print("🚀 [Scanner] 掃描全市場...")

    if not os.path.exists("stock_map.json"):
        print("❌ stock_map.json 不存在，請先執行 main.py")
        return

    with open("stock_map.json", "r", encoding="utf-8") as f:
        s_map = json.load(f)

    # 強化路徑搜尋
    files = glob.glob(os.path.join("data", "chip", "**", "*.parquet"), recursive=True)
    print(f"📁 找到 {len(files)} 個 parquet 檔案")

    if not files:
        print("❌ 掃描失敗：未偵測到 Parquet 檔案，請確認 data/chip/ 目錄")
        return

    # 修正：改用單線程迴圈，避免 GitHub Actions 上 multiprocessing hang 住
    results = []
    for f in files:
        r = process_file(f, s_map)
        if r:
            results.append(r)

    if results:
        out = pd.DataFrame(results)
        # 排序方便查看
        out = out.sort_values("大戶%", ascending=False).reset_index(drop=True)
        out.to_parquet("latest_snapshot.parquet", index=False)
        print(f"✅ 快照已更新：{len(results)} 檔標的")
    else:
        print("❌ 掃描結果為空：所有檔案均無法計算指標（可能歷史資料只有一期）")


if __name__ == "__main__":
    run_scan()
