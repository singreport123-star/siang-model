import pandas as pd
import requests
import io
import os
import json
import subprocess
import sys
import time
from datetime import datetime

# =========================
# 1. mapping (低頻更新)
# =========================
def update_mapping():
    print("🚀 [Siang-Model] mapping update...")

    urls = [
        "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2",
        "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4"
    ]

    mapping = {}
    headers = {'User-Agent': 'Mozilla/5.0'}

    for url in urls:
        try:
            res = requests.get(url, headers=headers, timeout=30)
            res.raise_for_status()

            tables = pd.read_html(io.StringIO(res.text))

            for df in tables:
                if "有價證券代號及名稱" not in df.columns:
                    continue

                for val in df["有價證券代號及名稱"].dropna():
                    if "　" in val:
                        code, name = val.split("　", 1)
                        mapping[code.strip()] = name.strip()

        except Exception as e:
            print(f"⚠ mapping warning: {e}")

    tmp = "stock_map.json.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)

    os.replace(tmp, "stock_map.json")

    print(f"✅ mapping done: {len(mapping)} 筆標的")


def ensure_mapping():
    if not os.path.exists("stock_map.json"):
        update_mapping()
        return

    # 超過7天才更新
    if time.time() - os.path.getmtime("stock_map.json") > 7 * 86400:
        update_mapping()


# =========================
# 2. TDCC 抓取
# =========================
def fetch_chip():
    print("🚀 [Siang-Model] TDCC fetch...")

    url = "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5"
    headers = {'User-Agent': 'Mozilla/5.0'}

    # retry 機制
    for i in range(3):
        try:
            res = requests.get(url, headers=headers, timeout=60)
            res.raise_for_status()
            df = pd.read_csv(io.BytesIO(res.content))
            break
        except Exception as e:
            print(f"⚠ retry {i+1}/3: {e}")
            if i == 2:
                print("❌ TDCC 下載失敗")
                return False

    if df.empty:
        print("❌ 空資料")
        return False

    df.columns = [c.replace('%', '').replace(' ', '').strip() for c in df.columns]
    df["證券代號"] = df["證券代號"].astype(str).str.strip()
    df = df[df["證券代號"].str.match(r"^[0-9A-Z]{4,6}$")]

    date = str(df["資料日期"].iloc[0])
    print(f"📅 Data Date: {date}")

    for sn, sub_df in df.groupby("證券代號"):
        folder = f"data/chip/{sn[:2]}"
        os.makedirs(folder, exist_ok=True)

        path = f"{folder}/{sn}.parquet"

        try:
            if os.path.exists(path):
                old = pd.read_parquet(path)
                combined = pd.concat([old, sub_df]).drop_duplicates(
                    subset=["資料日期", "持股分級"], keep="last"
                )
            else:
                combined = sub_df

            combined = combined.sort_values("資料日期")

            # atomic write
            tmp_path = path + ".tmp"
            combined.to_parquet(tmp_path, index=False)
            os.replace(tmp_path, path)

        except Exception as e:
            print(f"⚠ 寫入失敗 {sn}: {e}")

    print("✅ 數據同步完成")
    return True


# =========================
# 3. 呼叫 scanner
# =========================
def run_scanner():
    print("🚀 [Siang-Model] 啟動掃描器...")

    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        scanner_path = os.path.join(base_dir, "scanner.py")

        subprocess.run(
            [sys.executable, scanner_path],
            check=True
        )

        print("✅ 排行榜快照更新成功")

    except subprocess.CalledProcessError:
        print("❌ 掃描器執行失敗")

    except Exception as e:
        print(f"⚠ scanner error: {e}")


# =========================
# 主流程
# =========================
if __name__ == "__main__":
    start = datetime.now()

    ensure_mapping()   # 低頻更新 mapping

    ok = fetch_chip()  # 主資料

    if ok:
        run_scanner()  # 只有成功才跑
    else:
        print("❌ 資料更新失敗，跳過 scanner")

    print(f"🎯 總耗時: {datetime.now() - start}")
