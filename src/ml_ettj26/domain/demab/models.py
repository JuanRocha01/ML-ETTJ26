from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass(frozen=True)
class DemabInstrument:
    isin: str
    sigla: str
    emissao_date: date
    vencimento_date: date
    source: str = "BCB_DEMAB"


@dataclass(frozen=True)
class DemabQuoteDaily:
    trade_date: date
    isin: str

    pu_min: Optional[float]
    pu_med: Optional[float]
    pu_max: Optional[float]
    pu_lastro: Optional[float]
    valor_par: Optional[float]

    taxa_min: Optional[float]
    taxa_med: Optional[float]
    taxa_max: Optional[float]

    ref_month: str                # "YYYY-MM"
    raw_zip_file: str
    raw_zip_hash: str
    inner_file: str
    record_hash: str
    ingestion_ts_utc: str
