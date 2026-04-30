import pandas as pd
import glob
import os
import json
from multiprocessing import Pool


def calculate_metrics(df):
    if df.empty:
        return None

    df["資料日期"] = pd.to_datetime(df["資料日期"], errors="coerce")
    df = df.dropna(subset=["資料日期"])
    df = df.sort_values("資料日期")

    dates = df["資料日期"].unique()
    if len(dates) < 2:
        return None

    l_df = df[df["資料日期"] == dates[-1]]
    p_df = df[df["資料日期"] == dates[-2]]

    l_1000 = l_df.loc[l_df["持股分級"] == 15, "權重"].sum()
    p_1000 = p_df.loc[p_df["持股分級"] == 15, "權重"].sum()

    l_h = l_df["人數"].sum()
    p_h = p_df["人數"].sum()

    human_change = ((l_h - p_h) / p_h) * 100 if p_h > 0 else 0

    l_conc = l_df.loc[l_df["持股分級"] >= 11, "權重"].sum()

    return {
        "股號": None,
        "大戶%": round(l_1000, 2),
        "大戶週增減": round(l_1000 - p_1000, 2),
        "人數變動": round(human_change, 2),
        "集中度(大+中)": round(l_conc, 2)
    }


def process_file(args):
    f, stock_map = args
    sid = os.path.basename(f).replace(".parquet", "")

    try:
        df = pd.read_parquet(f)
        m = calculate_metrics(df)
        if m:
            m["股號"] = sid
            m["名稱"] = stock_map.get(sid, "未知")
            return m
    except:
        return None


def run_scan():
    print("🚀 scanning...")

    with open("stock_map.json", "r", encoding="utf-8") as f:
        stock_map = json.load(f)

    files = glob.glob("data/chip/**/*.parquet", recursive=True)

    with Pool(processes=2) as pool:
        results = pool.map(process_file, [(f, stock_map) for f in files])

    results = [r for r in results if r]

    if results:
        pd.DataFrame(results).to_parquet("latest_snapshot.parquet", index=False)
        print(f"✅ {len(results)} stocks updated")


if __name__ == "__main__":
    run_scan()
