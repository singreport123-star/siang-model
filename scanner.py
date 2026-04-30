"""
祥哥籌碼模型 v3.1 — 全市場掃描器
功能：掃描所有 parquet 檔，計算大戶/中間戶/散戶指標並輸出快照
改進：完整日誌、明確錯誤訊息、型別安全
"""

import glob
import json
import logging
import os
from pathlib import Path

import pandas as pd

# ── 日誌設定 ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

STOCK_MAP_PATH = Path("stock_map.json")
SNAPSHOT_PATH  = Path("latest_snapshot.parquet")
DATA_GLOB      = os.path.join("data", "chip", "**", "*.parquet")

# 大戶 = 15 級；中間戶 = 11-14 級
BIG_LEVEL  = 15
MID_LEVELS = list(range(11, 15))   # 11, 12, 13, 14


# ── 指標計算 ──────────────────────────────────────────────────────────────────
def calculate_metrics(df: pd.DataFrame) -> dict | None:
    """
    計算單一股票的籌碼指標。

    回傳 None 表示資料不足，無法計算。
    """
    if df.empty:
        return None

    required = ["資料日期", "持股分級", "權重", "人數"]
    if not all(c in df.columns for c in required):
        log.debug("缺少必要欄位：%s", [c for c in required if c not in df.columns])
        return None

    df = df.copy()
    df["資料日期"] = pd.to_datetime(df["資料日期"].astype(str), errors="coerce")
    df = df.dropna(subset=["資料日期"]).sort_values("資料日期")
    df["持股分級"] = pd.to_numeric(df["持股分級"], errors="coerce")
    df["權重"]     = pd.to_numeric(df["權重"],     errors="coerce").fillna(0.0)
    df["人數"]     = pd.to_numeric(df["人數"],     errors="coerce").fillna(0)

    dates = df["資料日期"].unique()
    if len(dates) < 2:
        return None

    latest_df = df[df["資料日期"] == dates[-1]]
    prev_df   = df[df["資料日期"] == dates[-2]]

    def _weight(frame: pd.DataFrame, levels) -> float:
        mask = frame["持股分級"].isin(levels if hasattr(levels, "__iter__") else [levels])
        return float(frame.loc[mask, "權重"].sum())

    l_big  = _weight(latest_df, [BIG_LEVEL])
    p_big  = _weight(prev_df,   [BIG_LEVEL])
    l_conc = _weight(latest_df, [BIG_LEVEL] + MID_LEVELS)

    l_holders = float(latest_df["人數"].sum())
    p_holders = float(prev_df["人數"].sum())
    holder_chg = round((l_holders - p_holders) / p_holders * 100, 2) if p_holders > 0 else 0.0

    return {
        "股號":          None,               # 由呼叫端填入
        "大戶%":         round(l_big, 2),
        "大戶週增減":    round(l_big - p_big, 2),
        "人數變動":      holder_chg,
        "集中度(大+中)": round(l_conc, 2),
    }


# ── 主掃描流程 ─────────────────────────────────────────────────────────────────
def run_scan() -> None:
    """掃描所有股票 parquet 並輸出全市場快照。"""
    log.info("🚀 開始全市場籌碼掃描...")

    if not STOCK_MAP_PATH.exists():
        log.error("❌ 找不到 stock_map.json，請先執行 main.py")
        return

    with STOCK_MAP_PATH.open(encoding="utf-8") as f:
        s_map: dict[str, str] = json.load(f)

    files = glob.glob(DATA_GLOB, recursive=True)
    if not files:
        log.warning("⚠️ 找不到任何 parquet 檔，請確認 data/chip/ 目錄")
        return

    log.info("📂 找到 %d 個股票檔案，開始計算...", len(files))
    results: list[dict] = []
    errors  = 0

    for fpath in files:
        sid = Path(fpath).stem
        try:
            df = pd.read_parquet(fpath)
            metrics = calculate_metrics(df)
            if metrics:
                metrics["股號"] = sid
                metrics["名稱"] = s_map.get(sid, "未知")
                results.append(metrics)
        except Exception as exc:  # noqa: BLE001
            log.warning("  ⚠ 跳過 %s：%s", sid, exc)
            errors += 1

    if not results:
        log.warning("⚠️ 無有效結果，快照未更新")
        return

    snapshot = pd.DataFrame(results)
    snapshot.to_parquet(SNAPSHOT_PATH, index=False)
    log.info(
        "✅ 快照已更新：%d 檔成功，%d 檔跳過 → %s",
        len(results), errors, SNAPSHOT_PATH,
    )


# ── 入口 ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_scan()
