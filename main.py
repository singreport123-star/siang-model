import pandas as pd
import requests
import io
import os
import json

def update_mapping():
    print("🚀 更新股票對照表...")

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


def fetch_chip():
    print("🚀 TDCC 下載中...")

    url = "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5"

    try:
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=60)
        df = pd.read_csv(io.BytesIO(res.content), encoding="utf-8", on_bad_lines="skip")
    except Exception as e:
        print(f"❌ download fail: {e}")
        return False

    if df.empty:
        return False

    df.columns = [str(c).strip().replace("%", "") for c in df.columns]

    rename = {}
    for c in df.columns:
        if "持股" in c and "權重" in c:
            rename[c] = "權重"
        if "人" in c:
            rename[c] = "人數"
        if "證券代號" in c:
            rename[c] = "證券代號"
        if "股數" in c:
            rename[c] = "股數"

    df = df.rename(columns=rename)

    df["證券代號"] = df["證券代號"].astype(str).str.strip()

    print(f"📅 {df['資料日期'].iloc[0]}")

    for sn, sub_df in df.groupby("證券代號"):
        folder = f"data/chip/{sn[:2]}"
        os.makedirs(folder, exist_ok=True)
        path = f"{folder}/{sn}.parquet"

        if os.path.exists(path):
            old = pd.read_parquet(path)
            combined = pd.concat([old, sub_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["資料日期", "持股分級"])
        else:
            combined = sub_df

        combined = combined.sort_values("資料日期")
        combined.to_parquet(path, index=False)

    print("✅ 完成")
    return True


if __name__ == "__main__":
    if not os.path.exists("stock_map.json"):
        update_mapping()
    fetch_chip()
