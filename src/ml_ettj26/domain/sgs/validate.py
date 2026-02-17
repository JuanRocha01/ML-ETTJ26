from __future__ import annotations

import pandas as pd


class DataQualityError(RuntimeError):
    pass


def validate_sgs_series_meta(df: pd.DataFrame) -> None:
    required = ["series_id", "series_name", "frequency", "unit", "source"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise DataQualityError(f"Missing columns in series_meta: {missing}")

    if df["series_id"].isna().any():
        raise DataQualityError("series_id has nulls in series_meta")

    if df.duplicated(subset=["series_id"]).any():
        raise DataQualityError("Duplicate series_id in series_meta")

    if not df["frequency"].isin(["D", "M"]).all():
        raise DataQualityError("Invalid frequency in series_meta (expected D or M)")


def validate_sgs_points(df: pd.DataFrame) -> None:
    required = ["series_id", "ref_date", "value", "record_hash", "raw_file", "raw_hash", "ingestion_ts_utc"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise DataQualityError(f"Missing columns in points: {missing}")

    if df["series_id"].isna().any():
        raise DataQualityError("series_id has nulls in points")
    if df["ref_date"].isna().any():
        raise DataQualityError("ref_date has nulls in points")
    if df["record_hash"].isna().any():
        raise DataQualityError("record_hash has nulls in points")

    if df.duplicated(subset=["series_id", "ref_date"]).any():
        raise DataQualityError("Duplicates on (series_id, ref_date) in points")
