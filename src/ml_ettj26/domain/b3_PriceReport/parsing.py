from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass
from datetime import datetime, date, timezone
from io import BufferedReader
from typing import Optional, Iterator, Sequence

import xml.etree.ElementTree as ET

from ml_ettj26.domain.b3_PriceReport.header_probe import parse_snapshot_ts_from_head
from ml_ettj26.domain.b3_PriceReport.models import DI1QuotesDaily


# ----------------------------
# XML choose: snapshot header + fallback by name suffix
# ----------------------------

_RE_TRAILING_INT_XML = re.compile(r"(\d+)(?=\.xml$)", re.IGNORECASE)

@dataclass(frozen=True)
class LatestXmlPick:
    xml_name: str
    snapshot_dt: Optional[datetime]
    method: str  # "header_ts" | "name_suffix" | "lexicographic"


@dataclass(frozen=True)
class XmlCandidate:
    xml_name: str
    snapshot_dt: Optional[datetime]
    suffix: Optional[int]

def trailing_int_from_xml_name(name: str) -> Optional[int]:
    m = _RE_TRAILING_INT_XML.search(name)
    return int(m.group(1)) if m else None

def _read_head(zi: zipfile.ZipFile, name: str, head_bytes: int) -> bytes:
    with zi.open(name) as f:
        return f.read(head_bytes)

def pick_latest_xml(
    zi: zipfile.ZipFile,
    xml_names: list[str],
    *,
    head_bytes: int = 64_000,
) -> LatestXmlPick:
    ranked = rank_xml_candidates(zi, xml_names, head_bytes=head_bytes)
    first = ranked[0]
    if first.snapshot_dt is not None:
        return LatestXmlPick(xml_name=first.xml_name, snapshot_dt=first.snapshot_dt, method="header_ts")
    if first.suffix is not None:
        return LatestXmlPick(xml_name=first.xml_name, snapshot_dt=None, method="name_suffix")
    return LatestXmlPick(xml_name=first.xml_name, snapshot_dt=None, method="lexicographic")


def rank_xml_candidates(
    zi: zipfile.ZipFile,
    xml_names: list[str],
    *,
    head_bytes: int = 64_000,
) -> list[XmlCandidate]:
    """
    Rankea XMLs por "mais recente" para fallback:
      1) snapshot_ts do header (desc)
      2) sufixo inteiro no nome (desc)
      3) nome lexicografico (desc)
    """
    if not xml_names:
        raise ValueError("xml_names vazio")

    candidates: list[XmlCandidate] = []
    for name in xml_names:
        head = _read_head(zi, name, head_bytes=head_bytes)
        candidates.append(
            XmlCandidate(
                xml_name=name,
                snapshot_dt=parse_snapshot_ts_from_head(head),
                suffix=trailing_int_from_xml_name(name),
            )
        )

    def _sort_key(c: XmlCandidate):
        return (
            c.snapshot_dt is not None,
            c.snapshot_dt or datetime.min.replace(tzinfo=timezone.utc),
            c.suffix is not None,
            c.suffix or -1,
            c.xml_name,
        )

    return sorted(candidates, key=_sort_key, reverse=True)


# ----------------------------
# Namespace-safe helpers: path-based extraction
# ----------------------------

def localname(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag

def find_text_path(elem: ET.Element, path: Sequence[str]) -> Optional[str]:
    """
    Busca texto seguindo uma sequência de tags por localname, ignorando namespaces.
    Ex: path=["FinInstrmAttrbts","AdjstdQtTax"]
    """
    cur = elem
    for p in path:
        nxt = None
        for child in list(cur):
            if localname(child.tag) == p:
                nxt = child
                break
        if nxt is None:
            return None
        cur = nxt
    return (cur.text or "").strip() if cur.text is not None else None

def parse_float(s: Optional[str]) -> Optional[float]:
    if s is None or s == "":
        return None
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return None

def parse_int(s: Optional[str]) -> Optional[int]:
    if s is None or s == "":
        return None
    try:
        return int(s)
    except ValueError:
        return None

def parse_trade_date(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        d = date.fromisoformat(s)  # YYYY-MM-DD
        return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    except ValueError:
        return None


# ----------------------------
# PricRpt -> DI1QuotesDaily (paths exatos do arquivo hierarquico xml)
# ----------------------------

_P_TRADDT = ("TradDt", "Dt")
_P_TCKR = ("SctyId", "TckrSymb")
_P_TRADQTY = ("TradDtls", "TradQty")

_P_OI = ("FinInstrmAttrbts", "OpnIntrst")
_P_FIQTY = ("FinInstrmAttrbts", "FinInstrmQty")

_P_BID = ("FinInstrmAttrbts", "BestBidPric")
_P_ASK = ("FinInstrmAttrbts", "BestAskPric")
_P_LAST = ("FinInstrmAttrbts", "LastPric")
_P_AVG = ("FinInstrmAttrbts", "TradAvrgPric")
_P_MIN = ("FinInstrmAttrbts", "MinPric")
_P_MAX = ("FinInstrmAttrbts", "MaxPric")

_P_ADJ_QT = ("FinInstrmAttrbts", "AdjstdQt")
_P_ADJ_TAX = ("FinInstrmAttrbts", "AdjstdQtTax")


def extract_di1_quote_from_pricrpt(
    pr: ET.Element,
    *,
    snapshot_ts_utc: datetime,
    lineage_id: str,
    ingestion_ts_utc: datetime,
) -> Optional[DI1QuotesDaily]:
    tck = find_text_path(pr, _P_TCKR)
    if not tck or not tck.startswith("DI1"):
        return None

    trad_dt = parse_trade_date(find_text_path(pr, _P_TRADDT))

    return DI1QuotesDaily(
        TradDt=trad_dt,
        TckrSymb=tck,
        snapshot_ts_utc=snapshot_ts_utc,
        AdjstdQtTax=parse_float(find_text_path(pr, _P_ADJ_TAX)),
        AdjstdQt=parse_float(find_text_path(pr, _P_ADJ_QT)),
        BestBidPric=parse_float(find_text_path(pr, _P_BID)),
        BestAskPric=parse_float(find_text_path(pr, _P_ASK)),
        LastPric=parse_float(find_text_path(pr, _P_LAST)),
        TradAvrgPric=parse_float(find_text_path(pr, _P_AVG)),
        MinPric=parse_float(find_text_path(pr, _P_MIN)),
        MaxPric=parse_float(find_text_path(pr, _P_MAX)),
        TradQty=parse_int(find_text_path(pr, _P_TRADQTY)),
        FinInstrmQty=parse_int(find_text_path(pr, _P_FIQTY)),
        OpnIntrst=parse_int(find_text_path(pr, _P_OI)),
        lineage_id=lineage_id,
        ingestion_ts_utc=ingestion_ts_utc,
    )


def iter_di1_quotes(
    file_obj: BufferedReader,
    *,
    snapshot_ts_utc: datetime,
    lineage_id: str,
    ingestion_ts_utc: datetime,
) -> Iterator[DI1QuotesDaily]:
    """
    Streaming: percorre o XML e emite só DI1 PricRpt.
    """
    ctx = ET.iterparse(file_obj, events=("end",))
    for event, elem in ctx:
        if localname(elem.tag) != "PricRpt":
            continue

        q = extract_di1_quote_from_pricrpt(
            elem,
            snapshot_ts_utc=snapshot_ts_utc,
            lineage_id=lineage_id,
            ingestion_ts_utc=ingestion_ts_utc,
        )
        elem.clear()  # crítico p/ memória

        if q is not None:
            yield q
