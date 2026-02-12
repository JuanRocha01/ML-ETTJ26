from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any

from ml_ettj26.extractors.bcb_raw import BcbSgsRawExtractor,BcbDemabNegociacoesRawExtractor


# ---------- Fakes (para testar sem internet e sem disco) ----------

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
        self.last_url: Optional[str] = None
        self.last_params: Optional[Dict[str, Any]] = None
        self.last_headers: Optional[Dict[str, str]] = None

    def get(self, url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None):
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
#-----------------------------TESTE 1----------------------------------
def test_sgs_fetch_and_store_builds_params_and_saves_bytes():
    # Arrange (prepara)
    fake_json = b'[{"data":"01/01/2020","valor":"1.23"}]'
    transport = FakeTransport(FakeResponse(content=fake_json, status_code=200))
    storage = MemoryStorage()

    extractor = BcbSgsRawExtractor(transport, storage)

    # Act (executa)
    out_path = extractor.fetch_and_store(
        series_id=433,
        start="01/01/2020",
        end="31/12/2020",
    )

    # Assert (verifica)
    assert transport.last_url == "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados"
    assert transport.last_params["formato"] == "json"
    assert transport.last_params["dataInicial"] == "01/01/2020"
    assert transport.last_params["dataFinal"] == "31/12/2020"

    # O path padrão tem o id e as datas
    assert out_path.startswith("bcb/sgs/433_01-01-2020_31-12-2020.json")
    assert storage.saved[out_path] == fake_json

#----------------------------------TESTE 2----------------------------------
from datetime import date

def test_demab_fetch_and_store_month_builds_url_and_saves_zip():
    # Arrange
    fake_zip = b"PK\x03\x04...."  # bytes que lembram zip (só simbólico)
    transport = FakeTransport(FakeResponse(content=fake_zip, status_code=200))
    storage = MemoryStorage()

    extractor = BcbDemabNegociacoesRawExtractor(transport, storage)

    # Act
    out_path = extractor.fetch_and_store_month(month=date(2024, 1, 1), tipo="T")

    # Assert
    assert transport.last_url == "https://www4.bcb.gov.br/pom/demab/negociacoes/download/NegT202401.ZIP"
    assert out_path == "bcb/demab/negociacoes/NegT202401.ZIP"
    assert storage.saved[out_path] == fake_zip

#----------------------------------TESTE 3----------------------------------
import pytest
from datetime import date

def test_demab_invalid_tipo_raises():
    transport = FakeTransport(FakeResponse(content=b"x", status_code=200))
    storage = MemoryStorage()
    extractor = BcbDemabNegociacoesRawExtractor(transport, storage)

    with pytest.raises(ValueError):
        extractor.fetch_and_store_month(month=date(2024, 1, 1), tipo="Z")

#----------------------------------TESTE 4----------------------------------
import pytest

def test_sgs_http_error_raises():
    transport = FakeTransport(FakeResponse(content=b"err", status_code=500))
    storage = MemoryStorage()
    extractor = BcbSgsRawExtractor(transport, storage)

    with pytest.raises(RuntimeError):
        extractor.fetch_and_store(series_id=433)

#----------------------------------TESTE 5----------------------------------
def test_sgs_custom_out_path_is_respected():
    fake_json = b"[]"
    transport = FakeTransport(FakeResponse(content=fake_json, status_code=200))
    storage = MemoryStorage()
    extractor = BcbSgsRawExtractor(transport, storage)

    out_path = extractor.fetch_and_store(series_id=433, out_path="custom/ipca.json")

    assert out_path == "custom/ipca.json"
    assert storage.saved["custom/ipca.json"] == fake_json

