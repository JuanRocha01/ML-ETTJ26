from __future__ import annotations

import pytest
from dataclasses import dataclass
from datetime import date
from typing import Optional, Dict, Any, List, Tuple

from ml_ettj26.extractors.bcb_demab_raw import DemabMonthlyRawExtractor, DemabConfig, DownloadError
from ml_ettj26.extractors.bcb_demab_specs import DEMAB_NEGOCIACOES


# ---------- Fakes (sem internet / sem disco) ----------

@dataclass
class FakeResponse:
    content: bytes
    status_code: int = 200

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP error {self.status_code}")


class FakeTransport:
    def __init__(self, response: FakeResponse):
        self.response = response
        self.calls: List[Tuple[str, Optional[Dict[str, Any]], Optional[Dict[str, str]]]] = []

        self.last_url: Optional[str] = None
        self.last_params: Optional[Dict[str, Any]] = None
        self.last_headers: Optional[Dict[str, str]] = None

    def get(self, url: str, params=None, headers=None):
        self.calls.append((url, params, headers))
        self.last_url = url
        self.last_params = params
        self.last_headers = headers
        return self.response


class MemoryStorage:
    def __init__(self):
        self.saved: Dict[str, bytes] = {}

    def save(self, relative_path: str, content: bytes) -> str:
        self.saved[relative_path] = content
        return relative_path


# ---------- Tests ----------
#-----------------------------------TESTE 1 -----------------------------------
def test_demab_fetch_and_store_single_month_returns_list_and_saves_bytes():
    fake_zip = b"PK\x03\x04...."
    transport = FakeTransport(FakeResponse(fake_zip))
    storage = MemoryStorage()

    extractor = DemabMonthlyRawExtractor(
        transport=transport,
        storage=storage,
        dataset=DEMAB_NEGOCIACOES,
        cfg=DemabConfig(base_url="https://www4.bcb.gov.br/pom/demab", default_out_dir="bcb/demab"),
    )

    # month pode ser qualquer dia do mês; internamente normaliza para 1º dia
    paths = extractor.fetch_and_store(tipo="T", month=date(2024, 1, 15))

    assert len(paths) == 1
    out_path = paths[0]

    # URL correta
    assert transport.last_url == "https://www4.bcb.gov.br/pom/demab/negociacoes/download/NegT202401.ZIP"

    # Path default (inclui dataset.key)
    assert out_path == "bcb/demab/negociacoes_titulos_federais_secundario/NegT202401.ZIP"
    assert storage.saved[out_path] == fake_zip

#-----------------------------------TESTE 2 -----------------------------------
def test_demab_fetch_and_store_single_month_with_out_dir_overrides_path_root():
    fake_zip = b"PK\x03\x04...."
    transport = FakeTransport(FakeResponse(fake_zip))
    storage = MemoryStorage()

    extractor = DemabMonthlyRawExtractor(
        transport=transport,
        storage=storage,
        dataset=DEMAB_NEGOCIACOES,
        cfg=DemabConfig(base_url="https://www4.bcb.gov.br/pom/demab", default_out_dir="bcb/demab"),
    )

    paths = extractor.fetch_and_store(tipo="E", month=date(2024, 2, 1), out_dir="custom/raw")

    assert paths == ["custom/raw/negociacoes_titulos_federais_secundario/NegE202402.ZIP"]
    assert transport.last_url == "https://www4.bcb.gov.br/pom/demab/negociacoes/download/NegE202402.ZIP"
    assert storage.saved[paths[0]] == fake_zip

#-----------------------------------TESTE 3 -----------------------------------
def test_demab_fetch_and_store_range_returns_multiple_paths_and_makes_multiple_requests():
    fake_zip = b"PK\x03\x04...."
    transport = FakeTransport(FakeResponse(fake_zip))
    storage = MemoryStorage()

    extractor = DemabMonthlyRawExtractor(
        transport=transport,
        storage=storage,
        dataset=DEMAB_NEGOCIACOES,
        cfg=DemabConfig(base_url="https://www4.bcb.gov.br/pom/demab", default_out_dir="bcb/demab"),
    )

    paths = extractor.fetch_and_store(
        tipo="T",
        start_month=date(2024, 1, 1),
        end_month=date(2024, 3, 1),
        out_dir="bcb/demab",
    )

    assert len(paths) == 3
    assert paths[0].endswith("/NegT202401.ZIP")
    assert paths[1].endswith("/NegT202402.ZIP")
    assert paths[2].endswith("/NegT202403.ZIP")

    # 3 meses => 3 requests
    assert len(transport.calls) == 3

    # cada arquivo foi salvo
    for p in paths:
        assert storage.saved[p] == fake_zip

#-----------------------------------TESTE 4 -----------------------------------
def test_demab_invalid_tipo_raises_value_error():
    transport = FakeTransport(FakeResponse(b"x"))
    storage = MemoryStorage()

    extractor = DemabMonthlyRawExtractor(
        transport=transport,
        storage=storage,
        dataset=DEMAB_NEGOCIACOES,
    )

    with pytest.raises(ValueError):
        extractor.fetch_and_store(tipo="Z", month=date(2024, 1, 1))

#-----------------------------------TESTE 5 -----------------------------------
def test_demab_invalid_param_combo_month_and_range_raises():
    transport = FakeTransport(FakeResponse(b"x"))
    storage = MemoryStorage()

    extractor = DemabMonthlyRawExtractor(
        transport=transport,
        storage=storage,
        dataset=DEMAB_NEGOCIACOES,
    )

    with pytest.raises(ValueError):
        extractor.fetch_and_store(
            tipo="T",
            month=date(2024, 1, 1),
            start_month=date(2024, 1, 1),
            end_month=date(2024, 2, 1),
        )

#-----------------------------------TESTE 6 -----------------------------------
def test_demab_range_missing_one_bound_raises():
    transport = FakeTransport(FakeResponse(b"x"))
    storage = MemoryStorage()

    extractor = DemabMonthlyRawExtractor(
        transport=transport,
        storage=storage,
        dataset=DEMAB_NEGOCIACOES,
    )

    with pytest.raises(ValueError):
        extractor.fetch_and_store(tipo="T", start_month=date(2024, 1, 1), end_month=None)  # type: ignore[arg-type]

#-----------------------------------TESTE 7 -----------------------------------
def test_demab_end_month_before_start_month_raises():
    transport = FakeTransport(FakeResponse(b"x"))
    storage = MemoryStorage()

    extractor = DemabMonthlyRawExtractor(
        transport=transport,
        storage=storage,
        dataset=DEMAB_NEGOCIACOES,
    )

    with pytest.raises(ValueError):
        extractor.fetch_and_store(tipo="T", start_month=date(2024, 2, 1), end_month=date(2024, 1, 1))

#-----------------------------------TESTE 8 -----------------------------------
def test_demab_http_error_is_wrapped_in_download_error_with_context():
    transport = FakeTransport(FakeResponse(b"err", status_code=404))
    storage = MemoryStorage()

    extractor = DemabMonthlyRawExtractor(
        transport=transport,
        storage=storage,
        dataset=DEMAB_NEGOCIACOES,
        cfg=DemabConfig(base_url="https://www4.bcb.gov.br/pom/demab", default_out_dir="bcb/demab"),
    )

    with pytest.raises(DownloadError) as excinfo:
        extractor.fetch_and_store(tipo="T", month=date(2024, 1, 1))

    msg = str(excinfo.value)
    assert "negociacoes_titulos_federais_secundario" in msg
    assert "tipo=T" in msg
    assert "mês=202401" in msg
    assert "https://www4.bcb.gov.br/pom/demab/negociacoes/download/NegT202401.ZIP" in msg
