from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd

from ml_ettj26.domain.bcb_demab.models import DemabInstrument, DemabQuoteDaily
from ml_ettj26.domain.bcb_demab.hashing import make_record_hash


EXPECTED_MAP = {
    "DATA MOV": "trade_date",
    "SIGLA": "sigla",
    "CODIGO ISIN": "isin",
    "EMISSAO": "emissao_date",
    "VENCIMENTO": "vencimento_date",
    "PU MIN": "pu_min",
    "PU MED": "pu_med",
    "PU MAX": "pu_max",
    "PU LASTRO": "pu_lastro",
    "VALOR PAR": "valor_par",
    "TAXA MIN": "taxa_min",
    "TAXA MED": "taxa_med",
    "TAXA MAX": "taxa_max",
}


def _parse_date_ptbr(s: str) -> datetime.date:
    return datetime.strptime(s, "%d/%m/%Y").date()


def _to_float_or_none(s: str) -> Optional[float]:
    s = (s or "").strip()
    if s == "":
        return None
    # aqui o pandas já leu tudo como str; vírgula pode aparecer
    s = s.replace(".", "").replace(",", ".") if "," in s else s
    try:
        return float(s)
    except ValueError:
        return None


def normalize_demab_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    # mantém só colunas conhecidas (ignora extras)
    cols_present = [c for c in EXPECTED_MAP.keys() if c in df_raw.columns]
    df = df_raw[cols_present].rename(columns=EXPECTED_MAP).copy()
    return df


def row_to_instrument(row, source="BCB_DEMAB") -> DemabInstrument:
    return DemabInstrument(
        isin=row["isin"].strip(),
        sigla=row["sigla"].strip(),
        emissao_date=_parse_date_ptbr(row["emissao_date"]),
        vencimento_date=_parse_date_ptbr(row["vencimento_date"]),
        source=source,
    )


def row_to_quote(row, *, ref_month: str, raw_zip_file: str, raw_zip_hash: str, inner_file: str, ingestion_ts_utc: str) -> DemabQuoteDaily:
    trade_date = _parse_date_ptbr(row["trade_date"])
    isin = row["isin"].strip()

    pu_min = _to_float_or_none(row.get("pu_min", ""))
    pu_med = _to_float_or_none(row.get("pu_med", ""))
    pu_max = _to_float_or_none(row.get("pu_max", ""))
    pu_lastro = _to_float_or_none(row.get("pu_lastro", ""))
    valor_par = _to_float_or_none(row.get("valor_par", ""))

    taxa_min = _to_float_or_none(row.get("taxa_min", ""))
    taxa_med = _to_float_or_none(row.get("taxa_med", ""))
    taxa_max = _to_float_or_none(row.get("taxa_max", ""))

    record_hash = make_record_hash(
        trade_date=trade_date,
        isin=isin,
        pu_med=pu_med,
        taxa_med=taxa_med,
    )

    return DemabQuoteDaily(
        trade_date=trade_date,
        isin=isin,
        pu_min=pu_min,
        pu_med=pu_med,
        pu_max=pu_max,
        pu_lastro=pu_lastro,
        valor_par=valor_par,
        taxa_min=taxa_min,
        taxa_med=taxa_med,
        taxa_max=taxa_max,
        ref_month=ref_month,
        raw_zip_file=raw_zip_file,
        raw_zip_hash=raw_zip_hash,
        inner_file=inner_file,
        record_hash=record_hash,
        ingestion_ts_utc=ingestion_ts_utc,
    )
