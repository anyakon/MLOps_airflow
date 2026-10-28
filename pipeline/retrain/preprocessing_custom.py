"""Собственный препроцессинг для переобучения в Airflow."""

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def haversine(lat1, lon1, lat2, lon2):
    rad = 6371
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return rad * c


def add_features_custom(df):
    df_ = df.copy()
    df_["dist_km"] = haversine(df_["lat"], df_["lon"], df_["merchant_lat"], df_["merchant_lon"])
    if "transaction_time" in df_.columns:
        df_["transaction_time"] = pd.to_datetime(df_["transaction_time"])
        df_["hour"] = df_["transaction_time"].dt.hour
        df_["dow"] = df_["transaction_time"].dt.dayofweek
        df_["is_weekend"] = (df_["transaction_time"].dt.dayofweek > 4).astype(int)
        df_["month"] = df_["transaction_time"].dt.month
    if "amount" in df_.columns:
        df_["amount_log"] = np.log1p(pd.to_numeric(df_["amount"], errors="coerce").fillna(0))
    return df_.drop(columns=["transaction_time", "name_1", "name_2", "street"], errors="ignore")


def run_preproc(input_df):
    output_df = add_features_custom(input_df)
    logger.info("Custom preproc done. Shape: %s", output_df.shape)
    return output_df
