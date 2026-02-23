from __future__ import annotations

import os
import re
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Tuple, List, Dict, Optional

import pandas as pd

from ml_ettj26.domain.b3_PriceReport.models import DataLineage, InstrumentMaster, DI1QuotesDaily
from ml_ettj26.domain.b3_PriceReport.zip_reader import NestedZipReader, sha256_zip_member
from ml_ettj26.domain.b3_PriceReport.parsing import pick_latest_xml, iter_di1_quotes
from ml_ettj26.domain.b3_PriceReport.header_probe import parse_snapshot_ts_from_head

import re
from datetime import datetime, timezone
from typing import Dict, Tuple

_RE_DI1 = re.compile(r"^DI1([FGHJKMNQUVXZ])(\d{2})$")

_MONTH_CODE = {
    "F": 1, "G": 2, "H": 3, "J": 4, "K": 5, "M": 6,
    "N": 7, "Q": 8, "U": 9, "V": 10, "X": 11, "Z": 12,
}

def build_first_bd_by_ym(bd_index_df) -> Dict[Tuple[int, int], datetime]:
    """
    bd_index_df: dataframe com colunas: date (datetime64), is_business_day (bool) e/ou bd_index
    Retorna dict (year, month) -> primeiro dia útil do mês (UTC midnight).
    """
    df = bd_index_df.copy()
    # garante datetime
    df["date"] = df["date"].dt.tz_localize("UTC") if df["date"].dt.tz is None else df["date"]
    df = df[df["is_business_day"] == True]  # só dias úteis
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    firsts = df.sort_values("date").groupby(["year", "month"], as_index=False).first()
    out = {}
    for _, r in firsts.iterrows():
        y, m = int(r["year"]), int(r["month"])
        # normaliza para datetime UTC (se vier Timestamp)
        dt = r["date"].to_pydatetime()
        out[(y, m)] = dt
    return out

def di1_maturity_from_ticker(ticker: str, first_bd_by_ym: Dict[Tuple[int,int], datetime]) -> datetime:
    """
    DI1F27 -> (month_code=F => Jan), year=2027 -> maturity = primeiro dia útil de Jan/2027.
    """
    m = _RE_DI1.match(ticker)
    if not m:
        raise ValueError(f"Ticker DI1 inválido: {ticker}")

    month_code = m.group(1)
    year = 2000 + int(m.group(2))
    month = _MONTH_CODE[month_code]
    try:
        return first_bd_by_ym[(year, month)]
    except KeyError:
        raise KeyError(f"bd_index não cobre {year}-{month:02d} para maturidade de {ticker}")


# PR200102_20200102.zip -> pega 20200102
_RE_YYYYMMDD = re.compile(r"_(\d{8})\.zip$", re.IGNORECASE)

def _month_key(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"

def _filter_zip_paths_for_month(zip_paths: List[str], year: int, month: int) -> List[str]:
    ym = f"{year:04d}{month:02d}"
    out = []
    for p in zip_paths:
        m = _RE_YYYYMMDD.search(os.path.basename(p))
        if not m:
            continue
        yyyymmdd = m.group(1)
        if yyyymmdd.startswith(ym):
            out.append(p)
    return sorted(out)

def _dataclasses_to_df(objs: List[object]) -> pd.DataFrame:
    if not objs:
        return pd.DataFrame()
    return pd.DataFrame([asdict(o) for o in objs])


def build_b3_di1_trusted_month(
    *,
    raw_zip_paths: List[str],
    bd_index_df: pd.DataFrame,
    year: int,
    month: int,
    head_bytes: int = 64_000,
    previous_instrument_master_df: Optional[pd.DataFrame] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Retorna:
      quotes_df (somente zips do mês),
      lineage_df (somente zips do mês),
      instrument_master_updated_df (GLOBAL: previous + novos do mês, dedup por TckrSymb)
    """

    month_zips = _filter_zip_paths_for_month(raw_zip_paths, year, month)
    ingestion_ts = datetime.now(timezone.utc)

    # O(1) maturity lookup
    first_bd_by_ym = build_first_bd_by_ym(bd_index_df)

    quotes: List[DI1QuotesDaily] = []
    lineages: List[DataLineage] = []
    new_instruments: Dict[str, InstrumentMaster] = {}

    for outer_zip_path in month_zips:
        outer_zip_name = os.path.basename(outer_zip_path)

        zr = NestedZipReader(outer_zip_path)
        with zr.open_inner_zip() as zi:
            inner_zip_name = "<inner_in_memory.zip>"  # se você quiser, dá pra expor o nome real no ZipReader

            xml_names = [n for n in zi.namelist() if n.lower().endswith(".xml")]
            pick = pick_latest_xml(zi, xml_names, head_bytes=head_bytes)
            xml_name = pick.xml_name

            # snapshot: se pick não achou, tenta header do escolhido; senão fallback para ingestion_ts (auditado via lineage)
            if pick.snapshot_dt is not None:
                snapshot_ts = pick.snapshot_dt
            else:
                head = zi.open(xml_name).read(head_bytes)
                snapshot_ts = parse_snapshot_ts_from_head(head) or ingestion_ts

            file_hash = sha256_zip_member(zi, xml_name)
            lineage_id = f"{outer_zip_name}|{inner_zip_name}|{xml_name}|{snapshot_ts.isoformat()}|{file_hash}"

            lineages.append(
                DataLineage(
                    lineage_id=lineage_id,
                    outer_zip=outer_zip_name,
                    inner_zip=inner_zip_name,
                    xml_name=xml_name,
                    snapshot_ts_utc=snapshot_ts.isoformat(),
                    hash_file=file_hash,
                    ingestion_ts_utc=ingestion_ts,
                )
            )

            with zi.open(xml_name) as f:
                for q in iter_di1_quotes(
                    f,
                    snapshot_ts_utc=snapshot_ts,
                    lineage_id=lineage_id,
                    ingestion_ts_utc=ingestion_ts,
                ):
                    quotes.append(q)

                    # Instrument master (GLOBAL, dedup)
                    tck = q.TckrSymb
                    if tck not in new_instruments:
                        maturity = di1_maturity_from_ticker(tck, first_bd_by_ym)
                        # DI1N26 -> asset DI1, month_code N, year 2026
                        new_instruments[tck] = InstrumentMaster(
                            TckrSymb=tck,
                            asset="DI1",
                            contract_month_code=tck[3],            # letra
                            contract_year=2000 + int(tck[4:6]),    # YY
                            maturity_date=maturity,
                        )

    quotes_df = _dataclasses_to_df(quotes)
    lineage_df = _dataclasses_to_df(lineages)
    new_instr_df = _dataclasses_to_df(list(new_instruments.values()))

    # Atualização global do InstrumentMaster
    if previous_instrument_master_df is None or previous_instrument_master_df.empty:
        instrument_master_updated_df = new_instr_df
    else:
        combined = pd.concat([previous_instrument_master_df, new_instr_df], ignore_index=True)
        # mantém o "último" em caso de conflito
        instrument_master_updated_df = combined.drop_duplicates(subset=["TckrSymb"], keep="last").reset_index(drop=True)

    return quotes_df, lineage_df, instrument_master_updated_df
