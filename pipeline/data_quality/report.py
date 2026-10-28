"""Генерация HTML-отчёта по результатам DQ-проверок."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from pipeline.settings import MODELS_DIR

logger = logging.getLogger(__name__)

DQ_REPORT_DIR = MODELS_DIR / "dq_reports"


def generate_dq_report(dq_results: dict) -> str:
    """Генерирует HTML-отчёт в директории рядом с моделями.
    Возвращает путь к отчёту.
    """
    DQ_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = dq_results.get("timestamp") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_path = DQ_REPORT_DIR / f"dq_report_{ts}.html"

    rows_html = []
    for r in dq_results.get("results", []):
        status = "PASS" if r.get("passed") else "FAIL"
        color = "#2ecc71" if r.get("passed") else "#e74c3c"
        rows_html.append(
            f"<tr><td>{r.get('rule')}</td><td style='color:{color};font-weight:bold'>{status}</td><td>{r.get('details')}</td></tr>"
        )

    html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <title>Data Quality Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        h1 {{ color: #2c3e50; }}
        .summary {{ background: #f8f9fa; padding: 20px; border-radius: 5px; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background-color: #34495e; color: white; }}
        tr:hover {{ background-color: #f5f5f5; }}
    </style>
</head>
<body>
    <h1>Отчёт Data Quality</h1>
    <div class="summary">
        <p><strong>Время проверки:</strong> {ts}</p>
        <p><strong>Проверено строк:</strong> {dq_results.get('rows_checked')}</p>
        <p><strong>Результат:</strong> {dq_results.get('passed')}/{dq_results.get('total')} правил пройдено</p>
        <p><strong>Все пройдены:</strong> {'Да' if dq_results.get('all_passed') else 'Нет'}</p>
    </div>
    <table>
        <thead>
            <tr><th>Правило</th><th>Статус</th><th>Детали</th></tr>
        </thead>
        <tbody>
            {''.join(rows_html)}
        </tbody>
    </table>
</body>
</html>"""

    report_path.write_text(html, encoding="utf-8")
    logger.info("HTML-отчёт сохранён: %s", report_path)
    return str(report_path)
