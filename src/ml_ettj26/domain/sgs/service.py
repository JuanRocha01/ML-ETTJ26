from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

import pandas as pd

from ml_ettj26.domain.sgs.models import SgsSeriesMeta
from ml_ettj26.domain.sgs.parsing import parse_series_id_from_filename, read_sgs_json
from ml_ettj26.domain.sgs.normalize import normalize_sgs_records
from ml_ettj26.utils.io.fs import file_sha256


@dataclass(frozen=True)
class SgsIngestConfig:
    raw_dir: str
    source: str = "BCB_SGS"
    series_meta: Dict[int, SgsSeriesMeta] | None = None
    on_conflict: str = "keep_last"


class SgsTrustedBuilder:
    """
    Constrói tabelas trusted:
      - dimensão: series_meta
      - fato: points (observações)
    """

    def __init__(self, config: SgsIngestConfig):
        self._cfg = config

    def build_series_meta_df(self) -> pd.DataFrame:
        meta = self._cfg.series_meta or {}
        rows = [
            {
                "series_id": m.series_id,
                "series_name": m.name,
                "frequency": m.frequency,
                "unit": m.unit,
                "source": m.source,
            }
            for m in meta.values()
        ]
        df = pd.DataFrame(rows).sort_values("series_id").reset_index(drop=True)
        if not df.empty:
            df["series_id"] = df["series_id"].astype("int64")
        return df

    def build_points_df(self) -> pd.DataFrame:
        raw_path = Path(self._cfg.raw_dir)
        files = sorted(raw_path.glob("*.json"))

        ingestion_ts_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        all_points = []

        for fp in files:
            series_id = parse_series_id_from_filename(fp)
            payload = read_sgs_json(fp)
            raw_hash = file_sha256(fp)

            pts = normalize_sgs_records(
                series_id=series_id,
                raw_file=fp.name,
                raw_hash=raw_hash,
                ingestion_ts_utc=ingestion_ts_utc,
                records=payload,
            )
            all_points.extend(pts)

        df = pd.DataFrame([p.__dict__ for p in all_points])

        # Tipos base
        df["series_id"] = df["series_id"].astype("int64")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

        # Conflitos (mesma chave com record_hash diferente)
        g = df.groupby(["series_id", "ref_date"])["record_hash"].nunique()
        conflicts = g[g > 1]
        if not conflicts.empty and self._cfg.on_conflict == "raise":
            sample = conflicts.index[:10].tolist()
            raise RuntimeError(f"Conflicts detected for (series_id, ref_date) with differing record_hash. Sample: {sample}")

        # Dedup por chave natural
        df = (
            df.sort_values(["series_id", "ref_date", "raw_file"])
              .drop_duplicates(subset=["series_id", "ref_date"], keep="last")
              .reset_index(drop=True)
        )

        return df
