from __future__ import annotations

from typing import Any, Dict, Mapping

from ml_ettj26.domain.sgs.models import SgsSeriesMeta


def parse_series_meta(series_meta_raw: Mapping[Any, Any], default_source: str) -> Dict[int, SgsSeriesMeta]:
    """
    Converte o bloco params['series_meta'] em dict[int, SgsSeriesMeta].

    Aceita chaves int ou str (YAML às vezes vira int, às vezes str).
    """
    out: Dict[int, SgsSeriesMeta] = {}

    for k, v in (series_meta_raw or {}).items():
        series_id = int(k)
        if not isinstance(v, Mapping):
            raise ValueError(f"series_meta[{k}] must be a mapping, got {type(v)}")

        name = str(v.get("name", "")).strip()
        freq = str(v.get("frequency", "")).strip().upper()
        unit = str(v.get("unit", "")).strip()
        source = str(v.get("source", default_source)).strip() or default_source

        out[series_id] = SgsSeriesMeta(
            series_id=series_id,
            name=name,
            frequency=freq,
            unit=unit,
            source=source,
        )

    return out
