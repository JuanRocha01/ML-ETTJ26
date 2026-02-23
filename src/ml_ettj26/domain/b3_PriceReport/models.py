from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass(frozen=True)
class DI1QuotesDaily:
    # Chave Primária
    TradDt : datetime
    TckrSymb : str
    snapshot_ts_utc: datetime

    AdjstdQtTax : float #taxa
    AdjstdQt : float    #pu
    

    # Info para futuros projetos
    BestBidPric: Optional[float]
    BestAskPric: Optional[float]
    LastPric: Optional[float]
    TradAvrgPric: Optional[float]
    MinPric: Optional[float]
    MaxPric: Optional[float]

    TradQty: Optional[int]
    FinInstrmQty: Optional[int]
    OpnIntrst: Optional[int]

    # Auditoria & Governança
    lineage_id : str # FK
    ingestion_ts_utc: datetime

@dataclass(frozen=True)
class InstrumentMaster:
    # PK
    TckrSymb : str

    asset: str
    contract_month_code: str
    contract_year: int
    maturity_date: datetime

@dataclass(frozen=True)
class DataLineage:
    # PK
    lineage_id : str # outer_zip|inner_zip|xml_name|snapshot_ts_utc|hash_file

    outer_zip: str
    inner_zip: str
    xml_name: str
    snapshot_ts_utc : str
    hash_file: str
    ingestion_ts_utc : datetime