from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class SwapDIxPRE:
    # Chave Primária
    TradDt : datetime
    CodProd : str
    days_to_maturity: int

    bd_to_maturity: int
    days_to_delivery: int

    raw_value: int
    raw_signal: str
    adjusted_value : float #taxa principal

    tipo_cotacao: str

    # Auditoria & Governança
    lineage_id : str # FK
    ingestion_ts_utc: datetime

@dataclass(frozen=True)
class SwapMaster:
    # PK
    CodProd : str

    nome: str
    underlying: Optional[str]
    fixed_leg: Optional[str]
    float_leg: Optional[str]
    calendar: Optional[str]


@dataclass(frozen=True)
class DataLineage:
    # PK
    lineage_id : str # sha256 hex de outer_zip|inner_zip|txt_name|hash_file

    outer_zip: str
    inner_zip: str
    txt_name: str
    hash_file: str
    
