from __future__ import annotations
from typing import Any, Mapping
import pandas as pd

from ml_ettj26.domain.demab.service import DemabIngestConfig, DemabTrustedBuilder
from ml_ettj26.domain.demab.validate import validate_demab_instruments, validate_demab_quotes

def build_demab_instruments(params: Mapping[str, Any]) -> pd.DataFrame:
    cfg = DemabIngestConfig(raw_dir=params["raw_dir"], source=params.get("source","BCB_DEMAB"))
    return DemabTrustedBuilder(cfg).build_instruments_df()

def build_demab_quotes(params: Mapping[str, Any]) -> pd.DataFrame:
    cfg = DemabIngestConfig(raw_dir=params["raw_dir"], source=params.get("source","BCB_DEMAB"))
    return DemabTrustedBuilder(cfg).build_quotes_df()
    
def build_demab_quotes_partitioned(params) -> dict[str, pd.DataFrame]:
    cfg = DemabIngestConfig(raw_dir=params["raw_dir"], source=params.get("source","BCB_DEMAB"))
    df = DemabTrustedBuilder(cfg).build_quotes_df()
    parts = {}
    for ref_month, g in df.groupby("ref_month", sort=True):
        parts[str(ref_month)] = g.drop(columns=["ref_month"])
    return parts

def validate_instruments(df: pd.DataFrame) -> pd.DataFrame:
    validate_demab_instruments(df); return df

def validate_quotes(df: pd.DataFrame) -> pd.DataFrame:
    validate_demab_quotes(df); return df
    
def validate_demab_quotes_partitioned(parts: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    for k, df in parts.items():
        validate_demab_quotes(df.assign(ref_month=k))  # se você dropou ref_month
    return parts
