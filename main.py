import pandas as pd
import requests
import io
import os
import json


def update_mapping():
    print("🚀 [Siang-Model] 更新股票對照表...")
    urls = [
        "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2",
        "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"
    ]
    mapping = {}
    headers = {'User-Agent': 'Mozilla/5.0'}
    for url in urls:
        try:
            res = requests.get(url, headers=headers, timeout=30)
            tables = pd.read_html(res.text)
            for df in tables:
                if df.shape[1] < 2:
                    continue
                for val in df.iloc[:, 0].dropna():
                    if isinstance(val, str) and "　" in val:
                        code, name = val.split("　", 1)
                        mapping[code.strip()] = name.strip()
        except Exception as e:
            print(f"⚠ mapping error: {e}")
    with open("stock_map.json", "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)
    print(f"✅ 股票對照表更新完成，共 {len(mapping)} 檔")


def fetch_chip():
    print("🚀 [Siang-Model] TDCC 下載中...")
    url = "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5"
    try:
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=60)
        df = pd.read_csv(io.BytesIO(res.content), encoding="utf-8", on_bad_lines="skip")
    except Exception as e:
        print(f"❌ 下載失敗: {e}")
        return False

    if df.empty:
        print("❌ 下載內容為空")
        return False

    # 清理欄位名稱
    df.columns = [str(c).strip().replace("%", "") for c in df.columns]
    print(f"📋 原始欄位: {list(df.columns)}")

    # 修正：改用 elif，避免同一欄位被多次覆寫
    # 同時補上「持股分級」的重命名（原版漏掉）
    rename_map = {}
    for c in df.columns:
        if "證券代號" in c:
            rename_map[c] = "證券代號"
        elif "持股" in c and "分級" in c:
            rename_map[c] = "持股分級"
        elif "持股" in c and "權重" in c:
            rename_map[c] = "權重"
        elif "人數" in c or ("人" in c and "數" in c):
            rename_map[c] = "人數"
        elif "股數" in c:
            rename_map[c] = "股數"
        elif "資料日期" in c or "日期" in c:
            rename_map[c] = "資料日期"

    df = df.rename(columns=rename_map)
    print(f"📋 重命名後欄位: {list(df.columns)}")

    # 確認關鍵欄位存在
    required_cols = ["證券代號", "持股分級", "資料日期"]
    for col in required_cols:
        if col not in df.columns:
            print(f"❌ 缺少關鍵欄位: {col}，請確認 TDCC 欄位格式")
            return False

    df["證券代號"] = df["證券代號"].astype(str).str.strip()

    date = str(df["資料日期"].iloc[0])
    print(f"📅 數據日期: {date}")

    count = 0
    for sn, sub_df in df.groupby("證券代號"):
        folder = f"data/chip/{sn[:2]}"
        os.makedirs(folder, exist_ok=True)
        path = f"{folder}/{sn}.parquet"
        if os.path.exists(path):
            try:
                old = pd.read_parquet(path)
                combined = pd.concat([old, sub_df], ignore_index=True)
                # 防呆：確認去重欄位存在
                dedup_cols = [c for c in ["資料日期", "持股分級"] if c in combined.columns]
                if dedup_cols:
                    combined = combined.drop_duplicates(subset=dedup_cols, keep="last")
            except Exception as e:
                print(f"⚠ 合併 {sn} 失敗，使用新資料: {e}")
                combined = sub_df
        else:
            combined = sub_df
        combined.sort_values("資料日期").to_parquet(path, index=False)
        count += 1

    print(f"✅ 數據同步完成，共寫入 {count} 檔標的")
    return True


if __name__ == "__main__":
    if not os.path.exists("stock_map.json"):
        update_mapping()
    fetch_chip()
