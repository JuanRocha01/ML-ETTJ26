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

# --- 1) helpers puros (fáceis de testar)
_RE_TRAILING_INT_XML = re.compile(r"(\d+)(?=\.xml$)", re.IGNORECASE)

def trailing_int_from_xml_name(name: str) -> Optional[int]:
    """Extrai o inteiro antes de .xml (ex: ...1830098585.xml -> 1830098585)."""
    m = _RE_TRAILING_INT_XML.search(name)
    return int(m.group(1)) if m else None

def pick_best_by_suffix(current_best: Optional[tuple[int, str]], name: str) -> Optional[tuple[int, str]]:
    """Atualiza o melhor (sufixo_int, nome) comparando pelo sufixo numérico."""
    suf = trailing_int_from_xml_name(name)
    if suf is None:
        return current_best
    if current_best is None or suf > current_best[0]:
        return (suf, name)
    return current_best

def pick_best_by_dt(current_best: Optional[tuple[datetime, str]], name: str, dt: Optional[datetime]) -> Optional[tuple[datetime, str]]:
    """Atualiza o melhor (dt, nome) comparando pelo datetime."""
    if dt is None:
        return current_best
    if current_best is None or dt > current_best[0]:
        return (dt, name)
    return current_best


# --- 2) função de I/O (única que lê bytes do ZipFile)
def read_xml_head(zi: zipfile.ZipFile, xml_name: str, head_bytes: int) -> bytes:
    with zi.open(xml_name) as f:
        return f.read(head_bytes)


# --- 3) “resultado” explícito 
@dataclass(frozen=True)
class LatestXmlPick:
    xml_name: str
    snapshot_dt: Optional[datetime]  # None se não encontrado no header
    method: str                      # "header_ts" | "name_suffix" | "lexicographic"


# --- 4) orquestrador 
def pick_latest_xml(
    zi: zipfile.ZipFile,
    xml_names: list[str],
    *,
    head_bytes: int = 16_384,
) -> LatestXmlPick:
    if not xml_names:
        raise ValueError("xml_names vazio")

    best_dt: Optional[tuple[datetime, str]] = None
    best_suffix: Optional[tuple[int, str]] = None
    best_lex = max(xml_names)

    for name in xml_names:
        best_suffix = pick_best_by_suffix(best_suffix, name)

        head = read_xml_head(zi, name, head_bytes=head_bytes)
        dt = parse_snapshot_ts_from_head(head)  # sua função existente
        best_dt = pick_best_by_dt(best_dt, name, dt)

    if best_dt is not None:
        dt, name = best_dt
        return LatestXmlPick(xml_name=name, snapshot_dt=dt, method="header_ts")

    if best_suffix is not None:
        _, name = best_suffix
        return LatestXmlPick(xml_name=name, snapshot_dt=None, method="name_suffix")

    return LatestXmlPick(xml_name=best_lex, snapshot_dt=None, method="lexicographic")
