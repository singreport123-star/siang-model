import pandas as pd
import glob
import os
import json

def calculate_metrics(df):
    if df.empty: return None
    # 確保欄位存在
    required = ["資料日期", "持股分級", "權重", "人數"]
    if not all(col in df.columns for col in required): return None

    df["資料日期"] = pd.to_datetime(df["資料日期"].astype(str), errors="coerce")
    df = df.dropna(subset=["資料日期"]).sort_values("資料日期")
    dates = df["資料日期"].unique()
    if len(dates) < 2: return None

    l_df = df[df["資料日期"] == dates[-1]]
    p_df = df[df["資料日期"] == dates[-2]]

    l_1000 = float(l_df.loc[l_df["持股分級"] == 15, "權重"].sum())
    p_1000 = float(p_df.loc[p_df["持股分級"] == 15, "權重"].sum())
    l_h = float(l_df["人數"].sum())
    p_h = float(p_df["人數"].sum())
    l_conc = float(l_df.loc[l_df["持股分級"] >= 11, "權重"].sum())

    return {
        "股號": None,
        "大戶%": round(l_1000, 2),
        "大戶週增減": round(l_1000 - p_1000, 2),
        "人數變動": round(((l_h - p_h) / p_h) * 100, 2) if p_h > 0 else 0,
        "集中度(大+中)": round(l_conc, 2)
    }

def run_scan():
    print("🚀 [Scanner] 掃描全市場...")
    if not os.path.exists("stock_map.json"): return
    with open("stock_map.json", "r", encoding="utf-8") as f:
        s_map = json.load(f)
    
    files = glob.glob(os.path.join("data", "chip", "**", "*.parquet"), recursive=True)
    results = []
    for f in files:
        sid = os.path.basename(f).replace(".parquet", "")
        try:
            df = pd.read_parquet(f)
            m = calculate_metrics(df)
            if m:
                m["股號"] = sid
                m["名稱"] = s_map.get(sid, "未知")
                results.append(m)
        except: continue

    if results:
        pd.DataFrame(results).to_parquet("latest_snapshot.parquet", index=False)
        print(f"✅ 快照已更新：{len(results)} 檔")

if __name__ == "__main__":
    run_scan()
