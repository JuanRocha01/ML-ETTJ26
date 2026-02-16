from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Iterable, List, Optional

from ml_ettj26.domain.sgs.models import SgsPoint
from ml_ettj26.domain.sgs.hashing import make_record_hash


def _parse_ptbr_date(s: str) -> "datetime.date":
    return datetime.strptime(s, "%d/%m/%Y").date()


def _parse_value_decimal(s: Optional[str]) -> Optional[Decimal]:
    if s is None:
        return None
    s = str(s).strip()
    if s == "":
        return None
    try:
        # SGS normalmente vem com ponto decimal.
        # Se algum dia vier com vírgula, isso aqui pode ser ajustado.
        return Decimal(s)
    except InvalidOperation:
        return None


def normalize_sgs_records(
    *,
    series_id: int,
    raw_file: str,
    raw_hash: str,
    ingestion_ts_utc: str,
    records: Iterable[dict],
) -> List[SgsPoint]:
    out: List[SgsPoint] = []
    for r in records:
        ref_date = _parse_ptbr_date(r["data"])
        value_dec = _parse_value_decimal(r.get("valor"))

        record_hash = make_record_hash(series_id, ref_date, value_dec)

        out.append(
            SgsPoint(
                series_id=series_id,
                ref_date=ref_date,
                value=float(value_dec) if value_dec is not None else None,
                record_hash=record_hash,    
                raw_file=raw_file,
                raw_hash=raw_hash,
                ingestion_ts_utc=ingestion_ts_utc,
            )
        )
    return out
