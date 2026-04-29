import pandas as pd
import requests
import io
import os
import json
from datetime import datetime

# =========================
# 1. mapping (解決 TWSE 格式不穩與漏標的顧慮)
# =========================
def update_mapping():
    print("🚀 [Siang-Model] mapping update...")
    
    urls = [
        "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2", # 上市
        "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"  # 上櫃
    ]
    
    mapping = {}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}

    for url in urls:
        try:
            # 使用 html5lib 作為備援解析器，解決 mapping 出現 0 筆的問題
            res = requests.get(url, headers=headers, timeout=30)
            tables = pd.read_html(io.StringIO(res.text), flavor='html5lib')
            
            for df in tables:
                # 祥哥顧慮：表格格式可能變動。
                # 策略：地毯式掃描所有儲存格內容。
                for val in df.astype(str).values.flatten():
                    # 支援全形或半形空白分隔的「代號 名稱」
                    if "　" in val or " " in val:
                        sep = "　" if "　" in val else " "
                        parts = val.split(sep, 1)
                        code, name = parts[0].strip(), parts[1].strip()
                        
                        # 只要代號符合 4~6 碼特徵 (含 00981A)
                        if 4 <= len(code) <= 6:
                            mapping[code] = name
        except Exception as e:
            print(f"⚠ mapping warning: {e}")

    # 存檔供網頁讀取
    with open("stock_map.json", "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)
    print(f"✅ mapping done: {len(mapping)} 筆標的")

# =========================
# 2. TDCC 抓取與 Parquet 封存 (解決重定向與不漏資料)
# =========================
def fetch_chip():
    print("🚀 [Siang-Model] TDCC fetch...")
    url = "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5"
    
    # 解決 Exceeded 30 redirects：模擬真實瀏覽器行為
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    }

    try:
        # allow_redirects=True 並增加更長的 timeout 預防爆掉
        res = requests.get(url, headers=headers, timeout=60, allow_redirects=True)
        res.raise_for_status()
        
        # 讀取 CSV
        df = pd.read_csv(io.BytesIO(res.content))
    except Exception as e:
        print(f"❌ download/parse fail: {e}")
        return

    if df.empty:
        return

    # 欄位標準化：移除 % 符號、移除空白，預防 Parquet 報錯
    df.columns = [c.replace('%', '').replace(' ', '').strip() for c in df.columns]
    df["證券代號"] = df["證券代號"].astype(str).str.strip()
    
    # 過濾非法標的，精確保留 4~6 碼 (包含帶字母的 00981A)
    df = df[df["證券代號"].str.match(r"^[0-9A-Z]{4,6}$")]
    
    date = str(df["資料日期"].iloc[0])
    print(f"📅 Data Date: {date}")

    # =========================
    # 3. 分層儲存 (不爆記憶體、不求快、只求穩)
    # =========================
    for sn, sub_df in df.groupby("證券代號"):
        folder = f"data/chip/{sn[:2]}"
        os.makedirs(folder, exist_ok=True)
        path = f"{folder}/{sn}.parquet"

        if os.path.exists(path):
            try:
                old = pd.read_parquet(path)
                # 核心去重：避免同一週重複存入
                combined = pd.concat([old, sub_df]).drop_duplicates(
                    subset=["資料日期", "持股分級"], keep="last"
                )
                combined.to_parquet(path, index=False, engine='pyarrow')
            except:
                sub_df.to_parquet(path, index=False, engine='pyarrow')
        else:
            sub_df.to_parquet(path, index=False, engine='pyarrow')

    print(f"✅ Siang Model 數據同步成功完成")

if __name__ == "__main__":
    start = datetime.now()
    update_mapping()
    fetch_chip()
    print(f"🎯 總耗時: {datetime.now() - start}")
