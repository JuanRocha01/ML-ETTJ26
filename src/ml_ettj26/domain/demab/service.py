from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import re

from ml_ettj26.utils.io.fs import file_sha256
from ml_ettj26.domain.demab.zip_reader import get_single_csv_name, open_csv_stream
from ml_ettj26.domain.demab.parsing import read_demab_csv
from ml_ettj26.domain.demab.normalize import normalize_demab_df, row_to_instrument, row_to_quote


@dataclass(frozen=True)
class DemabIngestConfig:
    raw_dir: str
    source: str = "BCB_DEMAB"


class DemabTrustedBuilder:
    def __init__(self, cfg: DemabIngestConfig):
        self._cfg = cfg

    def _iter_zips(self):
        raw_path = Path(self._cfg.raw_dir)
        zips = sorted(raw_path.glob("NegE*.ZIP"))  # estrito: prefixo e extensão

        if not zips:
            raise FileNotFoundError(f"No NegE*.ZIP files found in {raw_path}")

        # valida formato exato (opcional mas bom)
        import re
        for p in zips:
            if not re.fullmatch(r"NegE\d{6}\.ZIP", p.name):
                raise ValueError(f"Invalid DEMAB filename (expected NegEYYYYMM.ZIP): {p.name}")

        return zips

    def build_instruments_df(self) -> pd.DataFrame:
        ingestion_ts_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        instruments = {}

        for zp in self._iter_zips():
            inner = get_single_csv_name(zp)
            z, stream = open_csv_stream(zp, inner)
            try:
                df_raw = read_demab_csv(stream)
            finally:
                stream.close(); z.close()

            df = normalize_demab_df(df_raw)
            for _, r in df.iterrows():
                inst = row_to_instrument(r, source=self._cfg.source)
                instruments[inst.isin] = inst  # última ocorrência vence

        out = pd.DataFrame([i.__dict__ for i in instruments.values()])
        if not out.empty:
            out = out.sort_values("isin").reset_index(drop=True)
        return out

    def build_quotes_df(self) -> pd.DataFrame:
        ingestion_ts_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        quotes = []

        for zp in self._iter_zips():
            raw_zip_hash = file_sha256(zp)
            ref_month = self._infer_ref_month(zp.name)

            inner = get_single_csv_name(zp)
            z, stream = open_csv_stream(zp, inner)
            try:
                df_raw = read_demab_csv(stream)
            finally:
                stream.close(); z.close()

            df = normalize_demab_df(df_raw)

            for _, r in df.iterrows():
                q = row_to_quote(
                    r,
                    ref_month=ref_month,
                    raw_zip_file=zp.name,
                    raw_zip_hash=raw_zip_hash,
                    inner_file=inner,
                    ingestion_ts_utc=ingestion_ts_utc,
                )
                quotes.append(q)

        out = pd.DataFrame([q.__dict__ for q in quotes])

        # dedup por (trade_date, isin)
        out = (
            out.sort_values(["trade_date", "isin", "raw_zip_file"])
               .drop_duplicates(subset=["trade_date", "isin"], keep="last")
               .reset_index(drop=True)
        )
        return out

    def _infer_ref_month(self, zip_filename: str) -> str:
        # NegEYYYYMM.ZIP
        m = re.fullmatch(r"NegE(\d{4})(\d{2})\.ZIP", zip_filename, flags=re.IGNORECASE)
        if not m:
            raise ValueError(f"Invalid DEMAB NegE filename (expected NegEYYYYMM.ZIP): {zip_filename}")

        year, month = m.group(1), m.group(2)
        return f"{year}-{month}"
