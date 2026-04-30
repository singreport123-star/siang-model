import pandas as pd
import requests
import io
import os
import json

def update_mapping():
    print("🚀 [Siang-Model] 更新股票對照表...")
    urls = ["https://isin.twse.com.tw/isin/C_public.jsp?strMode=2", "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"]
    mapping = {}
    headers = {'User-Agent': 'Mozilla/5.0'}
    for url in urls:
        try:
            res = requests.get(url, headers=headers, timeout=30)
            tables = pd.read_html(res.text)
            for df in tables:
                if df.shape[1] < 2: continue
                for val in df.iloc[:, 0].dropna():
                    if isinstance(val, str) and "　" in val:
                        code, name = val.split("　", 1)
                        mapping[code.strip()] = name.strip()
        except Exception as e: print(f"⚠ mapping error: {e}")
    with open("stock_map.json", "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)

def fetch_chip():
    print("🚀 [Siang-Model] TDCC 下載中...")
    url = "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5"
    try:
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=60)
        df = pd.read_csv(io.BytesIO(res.content), encoding="utf-8", on_bad_lines="skip")
    except Exception as e:
        print(f"❌ 下載失敗: {e}"); return False

    if df.empty: return False

    # 絕對命名校準
    df.columns = [str(c).strip().replace("%", "") for c in df.columns]
    rename_map = {}
    for c in df.columns:
        if "證券代號" in c: rename_map[c] = "證券代號"
        elif "持股" in c and "分級" in c: rename_map[c] = "持股分級"
        elif "持股" in c and "權重" in c: rename_map[c] = "權重"
        elif "人數" in c or ("人" in c and "數" in c): rename_map[c] = "人數"
        elif "股數" in c: rename_map[c] = "股數"
        elif "資料日期" in c or "日期" in c: rename_map[c] = "資料日期"
    
    df = df.rename(columns=rename_map)
    df["證券代號"] = df["證券代號"].astype(str).str.strip()
    
    date = str(df["資料日期"].iloc[0])
    print(f"📅 數據日期: {date}")

    for sn, sub_df in df.groupby("證券代號"):
        folder = f"data/chip/{sn[:2]}"
        os.makedirs(folder, exist_ok=True)
        path = f"{folder}/{sn}.parquet"
        
        # 核心：確保合併時欄位一致
        if os.path.exists(path):
            try:
                old = pd.read_parquet(path)
                combined = pd.concat([old, sub_df], ignore_index=True)
                combined = combined.drop_duplicates(subset=["資料日期", "持股分級"], keep="last")
            except: combined = sub_df
        else:
            combined = sub_df
            
        combined.sort_values("資料日期").to_parquet(path, index=False)
    print("✅ 數據同步完成")
    return True

if __name__ == "__main__":
    if not os.path.exists("stock_map.json"): update_mapping()
    fetch_chip()
