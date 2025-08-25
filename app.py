# ============================================
# app.py  
# ============================================

import os, io, json
from datetime import datetime, timezone
from flask import Flask, jsonify

import pandas as pd
from google.cloud import storage

# ===== 你的核心邏輯，直接從本地腳本匯入 =====
# 將你現有的掃描檔案命名為 scanner_core.py，暴露 run_once()
# run_once() 內部：做你現在 Main 的所有事：掃描 -> 產生 candidates -> 選5筆 -> 回傳 (positions_df, todays_df, scan_snapshot)
import scanner_core  # <<== 把你原來的大腳本貼進去，底部改寫成 run_once() 回傳三個 DataFrame

BUCKET = os.environ.get("GCS_BUCKET", "")  # 你會在部署時設定
XLSX_KEY = os.environ.get("XLSX_KEY", "sim_trades.xlsx")

app = Flask(__name__)
gcs_client = storage.Client()

def load_positions_from_gcs() -> pd.DataFrame:
    """讀 GCS 的 Positions 分頁（第一次可能不存在）"""
    bucket = gcs_client.bucket(BUCKET)
    blob = bucket.blob(XLSX_KEY)
    if not blob.exists():
        return scanner_core.empty_positions_df()
    data = blob.download_as_bytes()
    try:
        return pd.read_excel(io.BytesIO(data), sheet_name="Positions")
    except Exception:
        return scanner_core.empty_positions_df()

def save_excel_to_gcs(positions: pd.DataFrame, todays: pd.DataFrame, scan_df: pd.DataFrame):
    """把三個分頁寫回 GCS 的同一個 xlsx"""
    bucket = gcs_client.bucket(BUCKET)
    blob = bucket.blob(XLSX_KEY)
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as w:
        positions.to_excel(w, index=False, sheet_name="Positions")
        todays.to_excel(w, index=False, sheet_name="TodaysPicks")
        if scan_df is not None and not scan_df.empty:
            scan_df.to_excel(w, index=False, sheet_name="ScanSnapshot")
    bio.seek(0)
    blob.upload_from_file(bio, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@app.get("/run")
def run_scan():
    # 從 GCS 載入既有部位（讓本輪能延續）
    base_positions = load_positions_from_gcs()
    positions, todays, scan_df = scanner_core.run_once(base_positions)
    save_excel_to_gcs(positions, todays, scan_df)

    return jsonify({
        "time_utc": datetime.now(timezone.utc).isoformat(),
        "selected": 0 if todays is None else int(len(todays)),
        "positions_rows": int(len(positions)),
        "xlsx": f"gs://{BUCKET}/{XLSX_KEY}"
    })

# 健康檢查
@app.get("/healthz")
def healthz():
    return "ok", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
