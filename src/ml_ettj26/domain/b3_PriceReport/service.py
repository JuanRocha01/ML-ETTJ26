from __future__ import annotations

import logging
import os
import re
import xml.etree.ElementTree as ET
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd

from ml_ettj26.domain.b3_PriceReport.header_probe import parse_snapshot_ts_from_head
from ml_ettj26.domain.b3_PriceReport.models import DataLineage, DI1QuotesDaily, InstrumentMaster
from ml_ettj26.domain.b3_PriceReport.parsing import iter_di1_quotes, rank_xml_candidates
from ml_ettj26.domain.b3_PriceReport.zip_reader import NestedZipReader, sha256_zip_member

_RE_DI1 = re.compile(r"^DI1([FGHJKMNQUVXZ])(\d{2})$")
_RE_YYYYMMDD = re.compile(r"_(\d{8})\.zip$", re.IGNORECASE)
logger = logging.getLogger(__name__)

_MONTH_CODE = {
    "F": 1,
    "G": 2,
    "H": 3,
    "J": 4,
    "K": 5,
    "M": 6,
    "N": 7,
    "Q": 8,
    "U": 9,
    "V": 10,
    "X": 11,
    "Z": 12,
}


def build_first_bd_by_ym(bd_index_df) -> Dict[Tuple[int, int], datetime]:
    """
    bd_index_df: dataframe com colunas date(datetime64) e is_business_day(bool).
    Retorna dict (year, month) -> primeiro dia util do mes (UTC midnight).
    """
    df = bd_index_df.copy()
    df["date"] = df["date"].dt.tz_localize("UTC") if df["date"].dt.tz is None else df["date"]
    df = df[df["is_business_day"] == True]
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    firsts = df.sort_values("date").groupby(["year", "month"], as_index=False).first()
    out: Dict[Tuple[int, int], datetime] = {}
    for _, row in firsts.iterrows():
        y, m = int(row["year"]), int(row["month"])
        out[(y, m)] = row["date"].to_pydatetime()
    return out


def di1_maturity_from_ticker(ticker: str, first_bd_by_ym: Dict[Tuple[int, int], datetime]) -> datetime:
    """
    DI1F27 -> month_code=F (Jan), year=2027 -> maturity = primeiro dia util de Jan/2027.
    """
    m = _RE_DI1.match(ticker)
    if not m:
        raise ValueError(f"Ticker DI1 invalido: {ticker}")

    month_code = m.group(1)
    year = 2000 + int(m.group(2))
    month = _MONTH_CODE[month_code]
    try:
        return first_bd_by_ym[(year, month)]
    except KeyError:
        raise KeyError(f"bd_index nao cobre {year}-{month:02d} para maturidade de {ticker}")


def _filter_zip_paths_for_month(zip_paths: List[str], year: int, month: int) -> List[str]:
    ym = f"{year:04d}{month:02d}"
    out = []
    for path in zip_paths:
        m = _RE_YYYYMMDD.search(os.path.basename(path))
        if not m:
            continue
        yyyymmdd = m.group(1)
        if yyyymmdd.startswith(ym):
            out.append(path)
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
      quotes_df (somente zips do mes),
      lineage_df (somente zips do mes),
      instrument_master_updated_df (GLOBAL: previous + novos do mes, dedup por TckrSymb)
    """
    month_zips = _filter_zip_paths_for_month(raw_zip_paths, year, month)
    ingestion_ts = datetime.now(timezone.utc)
    first_bd_by_ym = build_first_bd_by_ym(bd_index_df)

    quotes: List[DI1QuotesDaily] = []
    lineages: List[DataLineage] = []
    new_instruments: Dict[str, InstrumentMaster] = {}

    for outer_zip_path in month_zips:
        outer_zip_name = os.path.basename(outer_zip_path)
        zr = NestedZipReader(outer_zip_path)

        with zr.open_inner_zip() as zi:
            inner_zip_name = "<inner_in_memory.zip>"
            xml_names = [n for n in zi.namelist() if n.lower().endswith(".xml")]
            ranked = rank_xml_candidates(zi, xml_names, head_bytes=head_bytes)

            selected = None
            for rank_pos, cand in enumerate(ranked, start=1):
                xml_name = cand.xml_name
                snapshot_ts = cand.snapshot_dt
                if snapshot_ts is None:
                    head = zi.open(xml_name).read(head_bytes)
                    snapshot_ts = parse_snapshot_ts_from_head(head) or ingestion_ts

                file_hash = sha256_zip_member(zi, xml_name)
                lineage_id = f"{outer_zip_name}|{inner_zip_name}|{xml_name}|{snapshot_ts.isoformat()}|{file_hash}"

                candidate_quotes: List[DI1QuotesDaily] = []
                candidate_new_instruments: Dict[str, InstrumentMaster] = {}
                try:
                    with zi.open(xml_name) as f:
                        for q in iter_di1_quotes(
                            f,
                            snapshot_ts_utc=snapshot_ts,
                            lineage_id=lineage_id,
                            ingestion_ts_utc=ingestion_ts,
                        ):
                            candidate_quotes.append(q)
                            tck = q.TckrSymb
                            if tck not in new_instruments and tck not in candidate_new_instruments:
                                maturity = di1_maturity_from_ticker(tck, first_bd_by_ym)
                                candidate_new_instruments[tck] = InstrumentMaster(
                                    TckrSymb=tck,
                                    asset="DI1",
                                    contract_month_code=tck[3],
                                    contract_year=2000 + int(tck[4:6]),
                                    maturity_date=maturity,
                                )
                except ET.ParseError as exc:
                    logger.warning(
                        "XML parse falhou; tentando fallback. outer_zip=%s xml=%s rank=%s erro=%s",
                        outer_zip_name,
                        xml_name,
                        rank_pos,
                        exc,
                    )
                    continue

                selected = (
                    xml_name,
                    snapshot_ts,
                    file_hash,
                    lineage_id,
                    candidate_quotes,
                    candidate_new_instruments,
                    rank_pos,
                )
                break

            if selected is None:
                m = _RE_YYYYMMDD.search(outer_zip_name)
                day = m.group(1) if m else "unknown"
                logger.warning(
                    "Data pulada: nenhum XML parseavel no zip. outer_zip=%s day=%s",
                    outer_zip_name,
                    day,
                )
                continue

            (
                xml_name,
                snapshot_ts,
                file_hash,
                lineage_id,
                candidate_quotes,
                candidate_new_instruments,
                rank_pos,
            ) = selected

            if rank_pos > 1:
                logger.info(
                    "Fallback aplicado: selecionado XML parseavel mais antigo. outer_zip=%s xml=%s rank=%s",
                    outer_zip_name,
                    xml_name,
                    rank_pos,
                )

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
            quotes.extend(candidate_quotes)
            new_instruments.update(candidate_new_instruments)

    quotes_df = _dataclasses_to_df(quotes)
    lineage_df = _dataclasses_to_df(lineages)
    new_instr_df = _dataclasses_to_df(list(new_instruments.values()))

    if previous_instrument_master_df is None or previous_instrument_master_df.empty:
        instrument_master_updated_df = new_instr_df
    else:
        combined = pd.concat([previous_instrument_master_df, new_instr_df], ignore_index=True)
        instrument_master_updated_df = (
            combined.drop_duplicates(subset=["TckrSymb"], keep="last").reset_index(drop=True)
        )

    return quotes_df, lineage_df, instrument_master_updated_df
