from __future__ import annotations

import hashlib
from datetime import date
from typing import Optional


def _norm(x: Optional[float]) -> str:
    if x is None:
        return ""
    # formato estável
    return f"{x:.10f}"


def make_record_hash(*, trade_date: date, isin: str, pu_med: Optional[float], taxa_med: Optional[float]) -> str:
    payload = f"{trade_date.isoformat()}|{isin}|{_norm(pu_med)}|{_norm(taxa_med)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
