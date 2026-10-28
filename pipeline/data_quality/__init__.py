"""Логика проверок Data Quality.

Проверяет данные по 5+ правилам. Читает из БД/файлов, пишет результат
для генерации отчёта. Повторный запуск не должен ломать предыдущие отчёты.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from pipeline.db import get_conn
from pipeline.settings import MODELS_DIR

logger = logging.getLogger(__name__)

DQ_REPORT_DIR = MODELS_DIR / "dq_reports"


def _check_not_null(frame: pd.DataFrame, column: str) -> dict:
    if column not in frame.columns:
        return {"rule": f"not_null:{column}", "passed": False, "details": "column missing"}
    nulls = int(frame[column].isna().sum())
    passed = nulls == 0
    return {"rule": f"not_null:{column}", "passed": passed, "details": f"null_count={nulls}"}


def _check_positive(frame: pd.DataFrame, column: str) -> dict:
    if column not in frame.columns:
        return {"rule": f"positive:{column}", "passed": False, "details": "column missing"}
    try:
        bad = int((pd.to_numeric(frame[column], errors="coerce") <= 0).sum())
        passed = bad == 0
        return {"rule": f"positive:{column}", "passed": passed, "details": f"non_positive_or_nan_count={bad}"}
    except Exception as e:
        return {"rule": f"positive:{column}", "passed": False, "details": str(e)}


def _check_range(frame: pd.DataFrame, column: str, low: float, high: float) -> dict:
    if column not in frame.columns:
        return {"rule": f"range:{column}[{low},{high}]", "passed": False, "details": "column missing"}
    try:
        s = pd.to_numeric(frame[column], errors="coerce")
        bad = int(((s < low) | (s > high)).sum())
        passed = bad == 0
        return {"rule": f"range:{column}[{low},{high}]", "passed": passed, "details": f"out_of_range_count={bad}"}
    except Exception as e:
        return {"rule": f"range:{column}[{low},{high}]", "passed": False, "details": str(e)}


def _check_allowed_values(frame: pd.DataFrame, column: str, allowed: set) -> dict:
    if column not in frame.columns:
        return {"rule": f"allowed_values:{column}", "passed": False, "details": "column missing"}
    try:
        vals = set(frame[column].dropna().astype(str).unique())
        bad = vals - {str(a) for a in allowed}
        passed = len(bad) == 0
        return {"rule": f"allowed_values:{column}", "passed": passed, "details": f"unexpected_values={sorted(list(bad))[:10]}"}
    except Exception as e:
        return {"rule": f"allowed_values:{column}", "passed": False, "details": str(e)}


def _check_no_duplicates(frame: pd.DataFrame, key: str) -> dict:
    if key not in frame.columns:
        return {"rule": f"unique:{key}", "passed": False, "details": "column missing"}
    dup = int(frame.duplicated(subset=[key]).sum())
    passed = dup == 0
    return {"rule": f"unique:{key}", "passed": passed, "details": f"duplicate_count={dup}"}


def run_data_quality_checks() -> dict:
    """Запускает DQ-проверки. Читает сэмпл из labeled_transactions.
    Возвращает результаты проверок с временной меткой.
    """
    DQ_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_conn()
    try:
        # Берём сэмпл для быстрой проверки (не всё витрина)
        frame = pd.read_sql(
            """
            SELECT transaction_time, merch, cat_id, amount, name_1, name_2, gender,
                   street, one_city, us_state, post_code, lat, lon,
                   population_city, jobs, merchant_lat, merchant_lon, target
            FROM labeled_transactions
            ORDER BY random()
            LIMIT 100000
            """,
            conn,
        )
    finally:
        conn.close()

    results = []
    results.append(_check_not_null(frame, "transaction_time"))
    results.append(_check_not_null(frame, "amount"))
    results.append(_check_not_null(frame, "target"))
    results.append(_check_positive(frame, "amount"))
    results.append(_check_range(frame, "lat", -90, 90))
    results.append(_check_range(frame, "lon", -180, 180))
    results.append(_check_allowed_values(frame, "gender", {"M", "F", "MALE", "FEMALE"}))
    results.append(_check_no_duplicates(frame, "transaction_time"))  # не строго обязательно, но проверка
    # Сокращаем список до минимум 5, как требуют
    results = results[:8]

    passed = sum(1 for r in results if r["passed"])
    total = len(results)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = {
        "timestamp": ts,
        "rows_checked": int(len(frame)),
        "passed": passed,
        "total": total,
        "all_passed": passed == total,
        "results": results,
    }
    out_path = DQ_REPORT_DIR / f"dq_results_{ts}.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("DQ проверки: %s/%s пройдено", passed, total)
    return out
