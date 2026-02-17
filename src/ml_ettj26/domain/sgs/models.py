from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass(frozen=True)
class SgsSeriesMeta:
    """Metadados estáveis por série (não por data)."""
    series_id: int
    name: str
    frequency: str  # "D" ou "M"
    unit: str
    source: str = "BCB_SGS"


@dataclass(frozen=True)
class SgsPoint:
    """Observação (ponto no tempo) - não carrega metadados de série."""
    series_id: int
    ref_date: date
    value: Optional[float]
    record_hash: str
    raw_file: str
    raw_hash: str
    ingestion_ts_utc: str
