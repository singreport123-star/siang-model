import pandas as pd
import glob
import os
import json
from multiprocessing import Pool, cpu_count

# 對齊 main.py 的更名結果
REQUIRED_COLUMNS = [
    "資料日期",
    "持股分級",
    "權重",
    "人數",
]


def calculate_15x3_matrix(df: pd.DataFrame):
    """
    計算 15x3 籌碼矩陣
    """
    # 確保日期格式正確
    df["資料日期"] = pd.to_datetime(df["資料日期"], errors="coerce")
    df = df.dropna(subset=["資料日期"])

    if df["資料日期"].nunique() < 2:
        return None

    df = df.sort_values("資料日期")

    dates = df["資料日期"].unique()
    dates.sort()

    latest_date = dates[-1]
    prev_date = dates[-2]

    grouped = df.groupby("資料日期")

    latest_week = grouped.get_group(latest_date)
    prev_week = grouped.get_group(prev_date)

    # --- 計算 (使用校準後的欄位) ---
    l_1000 = latest_week.loc[latest_week["持股分級"] == 15, "權重"].sum()
    p_1000 = prev_week.loc[prev_week["持股分級"] == 15, "權重"].sum()
    diff_1000 = round(l_1000 - p_1000, 2)

    l_400 = latest_week.loc[latest_week["持股分級"] >= 11, "權重"].sum()
    p_400 = prev_week.loc[prev_week["持股分級"] >= 11, "權重"].sum()
    diff_400 = round(l_400 - p_400, 2)

    l_holders = latest_week["人數"].sum()
    p_holders = prev_week["人數"].sum()

    if p_holders <= 0:
        holder_change_rate = None
    else:
        holder_change_rate = round(((l_holders - p_holders) / p_holders) * 100, 2)

    return {
        "1000張變動": diff_1000,
        "400張變動": diff_400,
        "人數變動%": holder_change_rate,
        "最新1000張%": round(l_1000, 2),
    }


def process_file(args):
    f, stock_map = args

    stock_id = os.path.basename(f).replace(".parquet", "")

    # 僅保留個股與 ETF（4~6碼）
    if not (4 <= len(stock_id) <= 6):
        return None

    try:
        # 只讀取必要欄位，並確保名稱與 main.py 一致
        df = pd.read_parquet(f, columns=REQUIRED_COLUMNS)
        matrix = calculate_15x3_matrix(df)

        if matrix:
            matrix["代號"] = stock_id
            matrix["名稱"] = stock_map.get(stock_id, "未知")
            return matrix

    except Exception as e:
        # 如果舊資料欄位還沒更新，會在這邊噴錯是正常的，重新跑一次 main.py 就會修復
        print(f"⚠ 讀取 {stock_id} 失敗: {e}")

    return None


def run_scan():
    print("🚀 [Scanner] 掃描全市場 Parquet...")

    # 讀 stock_map
    try:
        with open("stock_map.json", "r", encoding="utf-8") as f:
            stock_map = json.load(f)
    except FileNotFoundError:
        stock_map = {}

    all_files = glob.glob("data/chip/**/*.parquet", recursive=True)

    # 多核心處理
    with Pool(cpu_count()) as pool:
        results = pool.map(process_file, [(f, stock_map) for f in all_files])

    rows = [r for r in results if r]

    if not rows:
        print("❌ 無資料或欄位尚未對齊（請先確保 main.py 執行成功）")
        return

    snapshot = pd.DataFrame(rows)

    # 強化排序（主力吸籌邏輯）
    snapshot = snapshot.sort_values(
        ["1000張變動", "400張變動", "人數變動%"],
        ascending=[False, False, True],
    )

    snapshot.to_parquet("latest_snapshot.parquet", index=False)

    print(f"✅ 完成：{len(rows)} 檔標的快照已產出")


if __name__ == "__main__":
    run_scan()
