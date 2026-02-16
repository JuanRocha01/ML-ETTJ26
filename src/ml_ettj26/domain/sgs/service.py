# src/ml_ettj26/domain/sgs/service.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import pandas as pd

from ml_ettj26.domain.sgs.parsing import parse_series_id_from_filename, read_sgs_json
from ml_ettj26.domain.sgs.normalize import normalize_sgs_records
from ml_ettj26.utils.io.fs import file_sha256


@dataclass(frozen=True)
class SgsIngestConfig:
    raw_dir: str  # e.g. "data/01_raw/bcb/sgs"
    source: str = "BCB_SGS"
    frequency: str = "D"


class SgsTrustedBuilder:
    """
    Single responsibility: construir o DataFrame trusted a partir do raw_dir.
    """

    def __init__(self, config: SgsIngestConfig):
        self._cfg = config

    def build(self) -> pd.DataFrame:
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

        # adiciona metadados de “negócio”
        df["source"] = self._cfg.source
        df["frequency"] = self._cfg.frequency

        # Dedup explícito (idempotência): se houver overlap entre arquivos
        # keep="last" porque seu ingestion_ts é o mesmo por run; ainda assim é uma política clara
        df = df.sort_values(["series_id", "ref_date", "raw_file"]).drop_duplicates(
                    subset=["series_id", "ref_date"],
                    keep="last",
                )

        # Tipos
        df["series_id"] = df["series_id"].astype("int64")
        # ref_date já é date python; parquet lida bem com isso via pyarrow
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

        return df
