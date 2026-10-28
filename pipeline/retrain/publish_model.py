"""Публикация бинарника модели и фиксация нового окна обучения."""

import json
import logging
import os
from datetime import date
from pathlib import Path

from airflow.exceptions import AirflowSkipException

from pipeline.db import advance_training_state, get_conn
from pipeline.settings import CURRENT_DATASET, MODELS_DIR

logger = logging.getLogger(__name__)


def _load_metrics(version: str) -> dict | None:
    metrics_path = MODELS_DIR / f"my_catboost_{version}.metrics.json"
    if not metrics_path.exists():
        return None
    try:
        return json.loads(metrics_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _get_current_version() -> str | None:
    latest_path = MODELS_DIR / "latest.txt"
    if not latest_path.exists():
        return None
    try:
        version = latest_path.read_text(encoding="utf-8").strip()
        return version or None
    except Exception:
        return None


def publish_model(train_info: dict) -> str:
    """Кладёт модель в общий каталог и только потом сдвигает указатель месяца.

    Перед публикацией сравнивает метрики новой модели с предыдущей версией.
    Если новая модель хуже по ROC-AUC (или F1 при отсутствии ROC-AUC) - не
    публикует, не обновляет latest.txt, не сдвигает окно и не обновляет
    training_state. При отсутствии предыдущей метрики публикует модель.
    """
    version = train_info["version"]
    staging_model = Path(train_info["staging_model"])
    final_model = MODELS_DIR / f"my_catboost_{version}.cbm"
    metrics_path = MODELS_DIR / f"my_catboost_{version}.metrics.json"

    if staging_model.exists():
        os.replace(staging_model, final_model)
    elif not final_model.exists():
        raise FileNotFoundError(f"Нет файла модели для публикации: {staging_model}")

    new_metrics = {
        "version": version,
        "batch_month": train_info["batch_month"],
        "train_months": train_info["train_months"],
        "test_month": train_info["test_month"],
        "train_rows": train_info["train_rows"],
        "test_rows": train_info["test_rows"],
        "roc_auc": train_info["roc_auc"],
        "f1": train_info["f1"],
    }
    metrics_path.write_text(
        json.dumps(new_metrics, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    current_version = _get_current_version()
    prev_metrics = _load_metrics(current_version) if current_version else None

    def is_better(new_m: dict, prev_m: dict) -> bool:
        new_roc = new_m.get("roc_auc")
        prev_roc = prev_m.get("roc_auc")
        if new_roc is not None and prev_roc is not None:
            if float(new_roc) > float(prev_roc) + 1e-12:
                return True
            if float(new_roc) < float(prev_roc) - 1e-12:
                return False
        new_f1 = new_m.get("f1") or 0.0
        prev_f1 = prev_m.get("f1") or 0.0
        if float(new_f1) > float(prev_f1) + 1e-12:
            return True
        return False

    if prev_metrics is not None and current_version != "initial":
        better = is_better(new_metrics, prev_metrics)
        if not better:
            logger.warning(
                "Новая модель %s хуже или равна %s. Не публикуем (prod останется на старой версии). roc_auc new=%s prev=%s f1 new=%s prev=%s",
                version,
                current_version,
                new_metrics.get("roc_auc"),
                prev_metrics.get("roc_auc"),
                new_metrics.get("f1"),
                prev_metrics.get("f1"),
            )
            try:
                if final_model.exists():
                    final_model.unlink()
            except Exception:
                pass
            try:
                if metrics_path.exists():
                    metrics_path.unlink()
            except Exception:
                pass
            raise AirflowSkipException("Новая модель не улучшила метрики, не публикуем")

    latest_tmp = MODELS_DIR / f".latest.{version}.txt"
    latest_tmp.write_text(version + "\n", encoding="utf-8")
    os.replace(latest_tmp, MODELS_DIR / "latest.txt")
    logger.info("Опубликована модель %s (%s)", version, final_model)

    pending_window = Path(train_info["dataset_path"])
    if pending_window.exists():
        os.replace(pending_window, CURRENT_DATASET)
        logger.info("Окно обучения обновлено: %s", CURRENT_DATASET)
    elif not CURRENT_DATASET.exists():
        raise FileNotFoundError(f"Нет нового окна обучения: {pending_window}")
    else:
        logger.info("Окно уже перенесено в %s, повторно не сдвигаем.", CURRENT_DATASET)

    batch_month = date.fromisoformat(train_info["batch_month"])
    conn = get_conn()
    try:
        advance_training_state(conn, batch_month)
    finally:
        conn.close()
    return version
