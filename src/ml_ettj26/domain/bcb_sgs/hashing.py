from __future__ import annotations

from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

from ml_ettj26.utils.io.hash import sha256_hex

_Q = Decimal("0.0000000001")  # 10 casas


def _normalize_decimal(value: Optional[Decimal]) -> str:
    if value is None:
        return ""
    # normaliza e quantiza determinísticamente
    vq = value.quantize(_Q, rounding=ROUND_HALF_UP)
    # remove notação científica e mantém string estável
    return format(vq, "f")


def make_record_hash(series_id: int, ref_date: date, value_dec: Optional[Decimal]) -> str:
    payload = f"{series_id}|{ref_date.isoformat()}|{_normalize_decimal(value_dec)}"
    return sha256_hex(payload)
