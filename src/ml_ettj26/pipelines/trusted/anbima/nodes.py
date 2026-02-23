from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Tuple

import pandas as pd

from ml_ettj26.utils.io.fs import file_sha256

def build_anbima_holidays_trusted(
    anbima_holidays_raw: pd.DataFrame,
    params: Mapping[str, Any],
) -> pd.DataFrame:
    """
    Saída: apenas feriados (1 linha por feriado).
    Colunas: cal_id, date, holiday_name, weekday, ingestion_ts_utc, source_file_hash, pipeline_run_id
    """
    cal_id = params.get("cal_id", "BR_ANBIMA")
    raw_csv_path = Path(params["raw_csv_path"])
    source_file_hash = file_sha256(raw_csv_path)
    pipeline_run_id = params.get("pipeline_run_id", "unknown")

    ingestion_ts_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    df = anbima_holidays_raw.copy()
    date_col = "Data"
    name_col = "Feriado"

    # parse date (ANBIMA normalmente é dd/mm/yyyy)
    df["date"] = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce", utc=True).dt.date
    df = df.dropna(subset=["date"]).copy()

    df["holiday_name"] = df[name_col].astype(str).str.strip()
    # normaliza "nan"/vazios
    df.loc[df["holiday_name"].str.lower().isin({"nan", "none", ""}), "holiday_name"] = None
    

    # weekday: 0=Mon .. 6=Sun
    df["weekday"] = pd.to_datetime(df["date"]).dt.weekday

    # dedup
    df = (
        df[["date", "holiday_name", "weekday"]]
        .drop_duplicates(subset=["date"], keep="last")
        .sort_values("date")
        .reset_index(drop=True)
    )

    df.insert(0, "cal_id", cal_id)
    df["date"] = pd.to_datetime(
        df["date"],
        dayfirst=True,
        errors="coerce",
        utc=True
        )

    df["ingestion_ts_utc"] = ingestion_ts_utc
    df["source_file_hash"] = source_file_hash
    df["pipeline_run_id"] = pipeline_run_id

    return df


def build_calendar_bd_index_trusted(
    holidays_trusted: pd.DataFrame,
    params: Mapping[str, Any],
) -> pd.DataFrame:
    """
    Saída: tabela diária com índice cumulativo de dias úteis (bd_index).
    Inclui holiday_name (null exceto feriados), weekday, is_business_day.
    Colunas: cal_id, date, weekday, is_business_day, bd_index, holiday_name, ingestion_ts_utc, source_file_hash, pipeline_run_id
    """
    cal_id = params.get("cal_id", "BR_ANBIMA")
    raw_csv_path = Path(params["raw_csv_path"])
    source_file_hash = file_sha256(raw_csv_path)
    pipeline_run_id = params.get("pipeline_run_id", "unknown")

    min_date = pd.to_datetime(params["min_date"]).date()
    max_date = pd.to_datetime(params["max_date"]).date()
    ingestion_ts_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    # mapa de feriados
    holiday_map = {}
    if not holidays_trusted.empty:
        holiday_map = dict(zip(holidays_trusted["date"], holidays_trusted["holiday_name"]))
            
    dates = pd.date_range(min_date, max_date, freq="D", tz="UTC")
    out = pd.DataFrame({"date": dates})
    out.insert(0, "cal_id", cal_id)
    out["weekday"] = pd.to_datetime(out["date"]).dt.weekday

    out["holiday_name"] = out["date"].map(holiday_map)
    is_holiday = out["holiday_name"].notna()
    out["is_business_day"] = (out["weekday"] < 5) & (~is_holiday)

    # bd_index cumulativo: conta dias úteis até a data (inclusive)
    # se você preferir bd_index começando em 0, troque o .cumsum() por .cumsum().astype(int) e subtraia 1 onde quiser
    out["bd_index"] = out["is_business_day"].astype("int64").cumsum()

    out["ingestion_ts_utc"] = ingestion_ts_utc
    out["source_file_hash"] = source_file_hash
    out["pipeline_run_id"] = pipeline_run_id

    return out[
        [
            "cal_id",
            "date",
            "weekday",
            "is_business_day",
            "bd_index",
            "holiday_name",
            "ingestion_ts_utc",
            "source_file_hash",
            "pipeline_run_id",
        ]
    ]
