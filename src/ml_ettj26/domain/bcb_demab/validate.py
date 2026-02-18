from __future__ import annotations
import pandas as pd

class DataQualityError(RuntimeError): ...

def validate_demab_instruments(df: pd.DataFrame) -> None:
    required = ["isin", "sigla", "emissao_date", "vencimento_date", "source"]
    miss = [c for c in required if c not in df.columns]
    if miss: raise DataQualityError(f"Missing columns in instruments: {miss}")
    if df["isin"].isna().any(): raise DataQualityError("isin nulls in instruments")
    if df.duplicated(subset=["isin"]).any(): raise DataQualityError("duplicate isin in instruments")

def validate_demab_quotes(df: pd.DataFrame) -> None:
    required = ["trade_date","isin","pu_med","taxa_med","ref_month","raw_zip_file","raw_zip_hash","inner_file","record_hash","ingestion_ts_utc"]
    miss = [c for c in required if c not in df.columns]
    if miss: raise DataQualityError(f"Missing columns in quotes: {miss}")
    if df["trade_date"].isna().any(): raise DataQualityError("trade_date nulls")
    if df["isin"].isna().any(): raise DataQualityError("isin nulls")
    if df.duplicated(subset=["trade_date","isin"]).any(): raise DataQualityError("duplicate (trade_date, isin)")
