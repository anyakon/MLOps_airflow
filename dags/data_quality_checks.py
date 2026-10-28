"""DAG проверок Data Quality датасета.

Файл только связывает задачи. Логика в pipeline.data_quality.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from pipeline.data_quality.checks import run_data_quality_checks
from pipeline.data_quality.report import generate_dq_report


@dag(
    dag_id="data_quality_checks",
    description="Проверки соответствия датасета Data Quality требованиям с публикацией HTML-отчёта",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=15),
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["mlops", "data-quality"],
    doc_md="""
### Data Quality проверки

Проверяет витрину labeled_transactions и/или текущее окно обучения по
минимуму 5 правилам DQ. Результаты публикуются в виде HTML-репорта в
отдельной директории рядом с директорией расположения модели (/opt/airflow/models).
    """,
)
def data_quality_checks():
    """Связывает запуск проверок и генерацию HTML-отчёта."""
    results = task(run_data_quality_checks, task_id="run_data_quality_checks")()
    task(generate_dq_report, task_id="generate_dq_report")(results)


data_quality_checks()
