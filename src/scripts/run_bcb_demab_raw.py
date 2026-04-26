from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import List

from ml_ettj26.utils.io.http import HTTPConfig, RequestsTransport
from ml_ettj26.utils.io.storage import LocalFileStorage
from ml_ettj26.extractors.bcb_demab_specs import DEMAB_NEGOCIACOES
from ml_ettj26.extractors.bcb_demab_raw import DemabMonthlyRawExtractor, DemabConfig


RAW_BASE_DIR = Path("data/01_raw")
OUT_DIR = "bcb/demab"  # diretório relativo (dentro do RAW_BASE_DIR)
OVERWRITE = False      # True => rebaixa mesmo se já existir


def _expected_relpath(tipo: str, year: int, month: int) -> str:
    """Monta o caminho relativo exatamente como o extractor fará com out_dir."""
    yyyymm = f"{year:04d}{month:02d}"
    tipo = tipo.upper()
    filename = f"Neg{tipo}{yyyymm}.ZIP"
    return f"{OUT_DIR}/{DEMAB_NEGOCIACOES.key}/{filename}"


def _month_iter(start_month: date, end_month: date):
    """Itera meses inclusivo (start..end), sempre no 1º dia do mês."""
    y, m = start_month.year, start_month.month
    end_y, end_m = end_month.year, end_month.month

    while (y < end_y) or (y == end_y and m <= end_m):
        yield y, m
        m += 1
        if m == 13:
            m = 1
            y += 1


def _download_tipo(extractor: DemabMonthlyRawExtractor, tipo: str, start: date, end: date) -> List[str]:
    """Baixa um tipo (T ou E) com skip por arquivo existente."""
    downloaded: List[str] = []
    skipped: int = 0

    for y, m in _month_iter(start, end):
        relpath = _expected_relpath(tipo=tipo, year=y, month=m)
        fullpath = RAW_BASE_DIR / relpath

        if fullpath.exists() and not OVERWRITE:
            skipped += 1
            continue

        # baixa 1 mês por vez (mantém logs e retentativas mais previsíveis)
        paths = extractor.fetch_and_store(
            tipo=tipo,
            month=date(y, m, 1),
            out_dir=OUT_DIR,
        )
        downloaded.extend(paths)

        print(f"[{tipo}] baixado: {paths[0]}")

    print(f"\nResumo [{tipo}]: {len(downloaded)} baixados, {skipped} pulados (já existiam).")
    return downloaded


def main():
    print("Iniciando extração DEMAB Negociações (T e/ou E) de 01/2007 até 01/2026")
    print(f"RAW_BASE_DIR: {RAW_BASE_DIR.resolve()}")
    print(f"OUT_DIR: {OUT_DIR}")
    print(f"OVERWRITE: {OVERWRITE}\n")

    # HTTP
    http_cfg = HTTPConfig(timeout_sec=60, max_retries=4, backoff_sec=1.0)
    transport = RequestsTransport(http_cfg)

    # Storage (em disco)
    storage = LocalFileStorage(str(RAW_BASE_DIR))

    extractor = DemabMonthlyRawExtractor(
        transport=transport,
        storage=storage,
        dataset=DEMAB_NEGOCIACOES,
        cfg=DemabConfig(base_url="https://www4.bcb.gov.br/pom/demab", default_out_dir="bcb/demab"),
    )

    start = date(2007, 1, 1)
    end = date(2026, 1, 1)

    # Extrai os dois tipos SEGUNDO BCB
    #downloaded_t = _download_tipo(extractor, tipo="T", start=start, end=end)
    downloaded_e = _download_tipo(extractor, tipo="E", start=start, end=end)

    print("\nConcluído!")
    #print(f"Total T baixados agora: {len(downloaded_t)}")
    print(f"Total E baixados agora: {len(downloaded_e)}")


if __name__ == "__main__":
    main()
