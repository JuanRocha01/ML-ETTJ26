from __future__ import annotations

import pandas as pd


class DataQualityError(RuntimeError):
    pass


def validate_sgs_daily(df: pd.DataFrame) -> None:

    required = ["series_id", "ref_date", "value", "record_hash", "raw_file", "raw_hash", "ingestion_ts_utc"]
    missing = [c for c in required if c not in df.columns]

    if missing:
        raise DataQualityError(f"Missing columns: {missing}")

    if df["series_id"].isna().any():
        raise DataQualityError("series_id has nulls")

    if df["ref_date"].isna().any():
        raise DataQualityError("ref_date has nulls")

    if df["value"].isna().any():
        raise DataQualityError("value has nulls")
    
    if df["record_hash"].isna().any():
        raise DataQualityError("record_hash has nulls")

    dup = df.duplicated(subset=["series_id", "ref_date"]).any()
    if dup:
        # não precisa falhar se você dedupar; mas eu prefiro falhar aqui
        # e dedupar explicitamente antes (pra ficar consciente)
        raise DataQualityError("Found duplicates on (series_id, ref_date)")
