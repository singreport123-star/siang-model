import pandas as pd
import requests
import io
import os
import json
from datetime import datetime

# =========================
# 1. mapping (祥哥特別顧慮優化版)
# =========================
def update_mapping():
    """解決 TWSE 格式不穩、沒全形空白或 HTML 變動的問題"""
    print("🚀 [Siang-Model] mapping update...")
    
    urls = [
        "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2", # 上市
        "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"  # 上櫃
    ]
    
    mapping = {}
    for url in urls:
        try:
            # 強制使用 lxml 且不依賴特定表格順序，掃描所有 table
            tables = pd.read_html(url)
            for df in tables:
                # 祥哥顧慮：表格可能分欄或沒全形空白
                # 策略：直接遍歷所有儲存格，只要符合「代號+名稱」特徵就抓
                for val in df.astype(str).values.flatten():
                    # 情況 A：標誌性的全形空白 (如 "2330　台積電")
                    if "　" in val:
                        parts = val.split("　", 1)
                        code, name = parts[0].strip(), parts[1].strip()
                    # 情況 B：部分格式可能變半形空白
                    elif " " in val:
                        parts = val.split(" ", 1)
                        code, name = parts[0].strip(), parts[1].strip()
                    else:
                        continue
                    
                    # 只要代號是 4~6 碼 (包含 00981A)，就收錄
                    if 4 <= len(code) <= 6:
                        mapping[code] = name
        except Exception as e:
            print(f"⚠ mapping warning: {e}")

    with open("stock_map.json", "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)
    print(f"✅ mapping done: {len(mapping)} 筆標的")

# =========================
# 2. TDCC 抓取與 Parquet 封存
# =========================
def fetch_chip():
    print("🚀 [Siang-Model] TDCC fetch...")
    url = "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5"

    try:
        res = requests.get(url, timeout=30)
        res.raise_for_status()
        # 處理可能的編碼問題
        df = pd.read_csv(io.BytesIO(res.content))
    except Exception as e:
        print(f"❌ download/parse fail: {e}")
        return

    if df.empty:
        return

    # 欄位標準化 (移除 % 符號避免 Parquet 報錯)
    df.columns = [c.replace('%', '').strip() for c in df.columns]
    df["證券代號"] = df["證券代號"].astype(str).str.strip()
    
    # 過濾非法代號，保留 4~6 碼 (含 A、T、D 等字尾)
    df = df[df["證券代號"].str.match(r"^[0-9A-Z]{4,6}$")]
    
    date = str(df["資料日期"].iloc[0])
    print(f"📅 Data Date: {date}")

    # =========================
    # 3. 分檔儲存 (不求快，求穩，不爆記憶體)
    # =========================
    for sn, sub_df in df.groupby("證券代號"):
        folder = f"data/chip/{sn[:2]}"
        os.makedirs(folder, exist_ok=True)
        path = f"{folder}/{sn}.parquet"

        if os.path.exists(path):
            try:
                old = pd.read_parquet(path)
                # 合併後去重，確保同一週資料不重複寫入
                combined = pd.concat([old, sub_df]).drop_duplicates(
                    subset=["資料日期", "持股分級"], keep="last"
                )
                combined.to_parquet(path, index=False, engine='pyarrow')
            except:
                sub_df.to_parquet(path, index=False, engine='pyarrow')
        else:
            sub_df.to_parquet(path, index=False, engine='pyarrow')

    print(f"✅ Siang Model 數據同步完成")

if __name__ == "__main__":
    start = datetime.now()
    update_mapping()
    fetch_chip()
    print(f"🎯 elapsed: {datetime.now() - start}")
