from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional, Dict, Any

import pytest

from ml_ettj26.extractors.b3_pregao_specs import PRICE_REPORT, SWAP_MARKET_RATES
from ml_ettj26.extractors.b3_pregao_raw import (
    B3PregaoDailyRawExtractor,
    B3PregaoHttpConfig,
    DownloadError,
)


# ----------------------------
# Fakes (test doubles)
# ----------------------------

class FakeResponse:
    def __init__(self, status_code: int, content: bytes = b"", raise_exc: Optional[Exception] = None):
        self.status_code = status_code
        self.content = content
        self._raise_exc = raise_exc

    def raise_for_status(self) -> None:
        if self._raise_exc is not None:
            raise self._raise_exc
        if 400 <= self.status_code:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeHttpTransport:
    """
    Simula o HttpTransport.get(url, params=...) retornando FakeResponse.
    Mapeia por filelist para escolher resposta.
    """
    def __init__(self, by_filelist: Dict[str, FakeResponse]):
        self.by_filelist = by_filelist
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, params: Optional[dict] = None):
        params = params or {}
        self.calls.append({"url": url, "params": params})
        filelist = params.get("filelist")
        if filelist not in self.by_filelist:
            # default: 404 se não estiver mapeado
            return FakeResponse(status_code=404, content=b"")
        return self.by_filelist[filelist]


class FakeByteStorage:
    """
    Simula ByteStorage.save(path, bytes) -> str.
    Armazena o último save para asserções.
    """
    def __init__(self):
        self.saved: list[tuple[str, bytes]] = []

    def save(self, path: str, content: bytes) -> str:
        self.saved.append((path, content))
        return path


# ----------------------------
# Tests
# ----------------------------

def _make_project_root(tmp_path: Path) -> Path:
    """
    Cria um "root" de projeto fake com pyproject.toml.
    (O extractor usa cfg.project_root diretamente, então não depende do util.)
    """
    (tmp_path / "pyproject.toml").write_text("[tool.poetry]\nname='x'\n", encoding="utf-8")
    return tmp_path


def test_default_out_path_points_to_project_data_root(tmp_path: Path):
    root = _make_project_root(tmp_path)

    cfg = B3PregaoHttpConfig(project_root=root, strict=False)
    transport = FakeHttpTransport(by_filelist={})
    storage = FakeByteStorage()

    ex = B3PregaoDailyRawExtractor(transport, storage, SWAP_MARKET_RATES, cfg)

    d = date(2020, 2, 13)
    out_path = ex._default_out_path(d)  # ok para teste de unidade

    expected = root / "data" / "01_raw" / "b3" / "DerivativesMarket-SwapMarketRates" / "TS200213_20200213.zip"
    assert Path(out_path) == expected


def test_fetch_once_success_saves_bytes(tmp_path: Path):
    root = _make_project_root(tmp_path)
    cfg = B3PregaoHttpConfig(project_root=root, strict=False)

    d = date(2021, 3, 2)
    filelist = PRICE_REPORT.build_filelist_param(d)

    transport = FakeHttpTransport(by_filelist={
        filelist: FakeResponse(status_code=200, content=b"ZIPBYTES"),
    })
    storage = FakeByteStorage()

    ex = B3PregaoDailyRawExtractor(transport, storage, PRICE_REPORT, cfg)
    saved_path = ex.fetch_and_store(day=d)

    assert len(saved_path) == 1
    assert len(storage.saved) == 1

    path, content = storage.saved[0]
    assert content == b"ZIPBYTES"
    expected = root / "data" / "01_raw" / "b3" / "PriceReport" / "PR210302_20210302.zip"
    assert Path(path) == expected


def test_fetch_once_404_non_strict_skips(tmp_path: Path):
    root = _make_project_root(tmp_path)
    cfg = B3PregaoHttpConfig(project_root=root, strict=False)

    d = date(2020, 2, 13)
    filelist = SWAP_MARKET_RATES.build_filelist_param(d)

    transport = FakeHttpTransport(by_filelist={
        filelist: FakeResponse(status_code=404, content=b""),
    })
    storage = FakeByteStorage()

    ex = B3PregaoDailyRawExtractor(transport, storage, SWAP_MARKET_RATES, cfg)
    paths = ex.fetch_and_store(day=d)

    assert paths == []
    assert storage.saved == []


def test_fetch_once_404_strict_raises(tmp_path: Path):
    root = _make_project_root(tmp_path)
    cfg = B3PregaoHttpConfig(project_root=root, strict=True)

    d = date(2020, 2, 13)
    filelist = SWAP_MARKET_RATES.build_filelist_param(d)

    transport = FakeHttpTransport(by_filelist={
        filelist: FakeResponse(status_code=404, content=b""),
    })
    storage = FakeByteStorage()

    ex = B3PregaoDailyRawExtractor(transport, storage, SWAP_MARKET_RATES, cfg)

    with pytest.raises(DownloadError):
        ex.fetch_and_store(day=d)


def test_fetch_and_store_range_skips_missing_days(tmp_path: Path):
    root = _make_project_root(tmp_path)
    cfg = B3PregaoHttpConfig(project_root=root, strict=False)

    # 2020-02-13 existe, 2020-02-14 não existe (simulado)
    d1 = date(2020, 2, 13)
    d2 = date(2020, 2, 14)

    f1 = SWAP_MARKET_RATES.build_filelist_param(d1)
    f2 = SWAP_MARKET_RATES.build_filelist_param(d2)

    transport = FakeHttpTransport(by_filelist={
        f1: FakeResponse(status_code=200, content=b"A"),
        f2: FakeResponse(status_code=404, content=b""),
    })
    storage = FakeByteStorage()

    ex = B3PregaoDailyRawExtractor(transport, storage, SWAP_MARKET_RATES, cfg)
    paths = ex.fetch_and_store(start_day=d1, end_day=d2)

    assert len(paths) == 1
    assert len(storage.saved) == 1
    assert storage.saved[0][1] == b"A"


def test_fetch_and_store_invalid_params_raise(tmp_path: Path):
    root = _make_project_root(tmp_path)
    cfg = B3PregaoHttpConfig(project_root=root, strict=False)

    transport = FakeHttpTransport(by_filelist={})
    storage = FakeByteStorage()

    ex = B3PregaoDailyRawExtractor(transport, storage, PRICE_REPORT, cfg)

    with pytest.raises(ValueError):
        ex.fetch_and_store(day=date(2021, 1, 1), start_day=date(2021, 1, 1), end_day=date(2021, 1, 2))

    with pytest.raises(ValueError):
        ex.fetch_and_store(start_day=date(2021, 1, 2), end_day=date(2021, 1, 1))

    with pytest.raises(ValueError):
        ex.fetch_and_store(start_day=date(2021, 1, 1), end_day=None)  # type: ignore[arg-type]
