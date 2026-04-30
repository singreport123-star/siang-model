"""
祥哥籌碼模型 v3.1 — 資料下載核心
功能：更新股票對照表 + 下載 TDCC 籌碼資料並存入 parquet
改進：加入重試機制、進度輸出、欄位校準更穩健
"""

import pandas as pd
import requests
import io
import os
import json
import time
import logging
from pathlib import Path

# ── 日誌設定 ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SiangModel/3.1)"}
DATA_DIR = Path("data/chip")
STOCK_MAP_PATH = Path("stock_map.json")


# ── 工具函式 ──────────────────────────────────────────────────────────────────
def http_get(url: str, retries: int = 3, timeout: int = 60, **kwargs) -> requests.Response:
    """帶重試的 GET 請求。"""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            log.warning("第 %d 次請求失敗 (%s)：%s", attempt, url, exc)
            if attempt < retries:
                time.sleep(attempt * 3)
    raise RuntimeError(f"連線失敗，已重試 {retries} 次：{url}")


# ── 股票對照表 ─────────────────────────────────────────────────────────────────
def update_mapping() -> None:
    """從 TWSE / TPEx ISIN 查詢頁面更新股票代號對照表。"""
    log.info("🚀 更新股票對照表...")
    urls = [
        "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2",
        "https://isin.twse.com.tw/isin/C_public.jsp?strMode=4",
    ]
    mapping: dict[str, str] = {}

    for url in urls:
        try:
            resp = http_get(url, timeout=30)
            tables = pd.read_html(io.StringIO(resp.text))
            for df in tables:
                if df.shape[1] < 2:
                    continue
                for val in df.iloc[:, 0].dropna():
                    if isinstance(val, str) and "\u3000" in val:
                        code, name = val.split("\u3000", 1)
                        mapping[code.strip()] = name.strip()
        except Exception as exc:  # noqa: BLE001
            log.error("對照表更新失敗 (%s)：%s", url, exc)

    if mapping:
        STOCK_MAP_PATH.write_text(
            json.dumps(mapping, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        log.info("✅ 股票對照表已更新：%d 筆", len(mapping))
    else:
        log.warning("⚠️ 對照表為空，略過寫入")


# ── 欄位校準 ──────────────────────────────────────────────────────────────────
_COLUMN_RULES: list[tuple[str, list[str]]] = [
    ("資料日期", ["資料日期", "日期"]),
    ("證券代號", ["證券代號", "股票代號", "代號"]),
    ("持股分級", ["持股分級", "持股等級", "分級"]),
    ("權重",     ["持股比例", "權重", "比例"]),
    ("人數",     ["人數"]),
    ("股數",     ["股數"]),
]

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """將來源欄位統一對應到標準欄位名稱。"""
    # 先清除百分比符號與前後空白
    df.columns = [str(c).strip().replace("%", "") for c in df.columns]
    rename_map: dict[str, str] = {}
    for target, candidates in _COLUMN_RULES:
        for col in df.columns:
            if any(kw in col for kw in candidates) and col not in rename_map:
                rename_map[col] = target
                break
    return df.rename(columns=rename_map)


# ── 主下載流程 ─────────────────────────────────────────────────────────────────
def fetch_chip() -> bool:
    """下載 TDCC 持股分佈資料並依股票代號分批存入 parquet。"""
    log.info("🚀 開始下載 TDCC 籌碼資料...")
    url = "https://opendata.tdcc.com.tw/getOD.ashx?id=1-5"

    try:
        resp = http_get(url)
        df = pd.read_csv(io.BytesIO(resp.content), encoding="utf-8", on_bad_lines="skip")
    except Exception as exc:  # noqa: BLE001
        log.error("❌ 下載或解析失敗：%s", exc)
        return False

    if df.empty:
        log.warning("⚠️ 下載資料為空")
        return False

    df = _normalize_columns(df)

    # 必要欄位檢查
    required = ["資料日期", "證券代號", "持股分級", "權重", "人數", "股數"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        log.error("❌ 缺少必要欄位：%s（現有欄位：%s）", missing, list(df.columns))
        return False

    # 型別轉換
    df["證券代號"] = df["證券代號"].astype(str).str.strip()
    df["持股分級"] = pd.to_numeric(df["持股分級"], errors="coerce")
    df["權重"]     = pd.to_numeric(df["權重"],     errors="coerce").fillna(0.0)
    df["人數"]     = pd.to_numeric(df["人數"],     errors="coerce").fillna(0)
    df["股數"]     = pd.to_numeric(df["股數"],     errors="coerce").fillna(0)
    df = df.dropna(subset=["持股分級"])

    date_val = df["資料日期"].iloc[0]
    log.info("📅 資料日期：%s", date_val)

    groups = list(df.groupby("證券代號"))
    total = len(groups)
    log.info("📦 共 %d 檔股票，開始寫入...", total)

    for i, (sid, sub_df) in enumerate(groups, 1):
        folder = DATA_DIR / str(sid)[:2]
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{sid}.parquet"

        if path.exists():
            try:
                old = pd.read_parquet(path)
                combined = pd.concat([old, sub_df], ignore_index=True)
                combined = combined.drop_duplicates(
                    subset=["資料日期", "持股分級"], keep="last"
                )
            except Exception as exc:  # noqa: BLE001
                log.warning("合併失敗 (%s)，以新資料覆寫：%s", sid, exc)
                combined = sub_df
        else:
            combined = sub_df

        combined.sort_values("資料日期").to_parquet(path, index=False)

        if i % 500 == 0 or i == total:
            log.info("  進度：%d / %d (%.1f%%)", i, total, i / total * 100)

    log.info("✅ 數據同步完成")
    return True


# ── 入口 ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not STOCK_MAP_PATH.exists():
        update_mapping()
    fetch_chip()
