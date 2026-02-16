from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass(frozen=True)
class SgsPoint:
    series_id: int
    ref_date: date
    value: Optional[float]  # pode ser None se vier vazio
    raw_file: str
    raw_hash: str
    record_hash: str
    ingestion_ts_utc: str   # ISO string em UTC
