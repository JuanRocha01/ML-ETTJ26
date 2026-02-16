import pytest
from datetime import date

from ml_ettj26.extractors.bcb_sgs_raw import BcbSgsRawExtractor


# ---------- fakes ----------
class FakeResponse:
    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP error {self.status_code}")


class FakeTransport:
    def __init__(self, response: FakeResponse):
        self.response = response
        self.calls = []  # guarda todas as chamadas (não só a última)

        self.last_url = None
        self.last_params = None

    def get(self, url: str, params=None, headers=None):
        self.calls.append((url, params, headers))
        self.last_url = url
        self.last_params = params
        return self.response


class MemoryStorage:
    def __init__(self):
        self.saved = {}

    def save(self, relative_path: str, content: bytes) -> str:
        self.saved[relative_path] = content
        return relative_path


class FakeSplitter:
    def split(self, start: str, end: str):
        # força 2 chunks fixos
        return [("01/01/2020", "31/12/2020"), ("01/01/2021", "31/12/2021")]


# ---------- tests SGS ----------
def test_sgs_fetch_and_store_single_range_returns_list_and_saves_bytes():
    fake_json = b'[{"data":"01/01/2020","valor":"1.23"}]'
    transport = FakeTransport(FakeResponse(fake_json))
    storage = MemoryStorage()

    extractor = BcbSgsRawExtractor(transport, storage)

    paths = extractor.fetch_and_store(series_id=433, start="01/01/2020", end="31/12/2020")
    assert len(paths) == 1

    out_path = paths[0]
    assert transport.last_url == "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados"
    assert transport.last_params["formato"] == "json"
    assert transport.last_params["dataInicial"] == "01/01/2020"
    assert transport.last_params["dataFinal"] == "31/12/2020"
    assert storage.saved[out_path] == fake_json


def test_sgs_uses_injected_splitter_and_creates_multiple_files():
    fake_json = b"[]"
    transport = FakeTransport(FakeResponse(fake_json))
    storage = MemoryStorage()

    extractor = BcbSgsRawExtractor(transport, storage, splitter=FakeSplitter())

    paths = extractor.fetch_and_store(series_id=432, start="01/01/2001", end="31/12/2021", out_dir="bcb/sgs")

    assert len(paths) == 2
    assert paths[0] == "bcb/sgs/432_01-01-2020_31-12-2020.json"
    assert paths[1] == "bcb/sgs/432_01-01-2021_31-12-2021.json"
    assert storage.saved[paths[0]] == fake_json
    assert storage.saved[paths[1]] == fake_json
    assert len(transport.calls) == 2  # duas requisições


def test_sgs_without_dates_makes_one_request():
    fake_json = b"[]"
    transport = FakeTransport(FakeResponse(fake_json))
    storage = MemoryStorage()

    extractor = BcbSgsRawExtractor(transport, storage)

    paths = extractor.fetch_and_store(series_id=432)
    assert len(paths) == 1
    assert transport.last_params == {"formato": "json"}


def test_sgs_start_without_end_raises():
    transport = FakeTransport(FakeResponse(b"[]"))
    storage = MemoryStorage()
    extractor = BcbSgsRawExtractor(transport, storage)

    with pytest.raises(ValueError):
        extractor.fetch_and_store(series_id=432, start="01/01/2020", end=None)


def test_sgs_http_error_raises():
    transport = FakeTransport(FakeResponse(b"err", status_code=500))
    storage = MemoryStorage()
    extractor = BcbSgsRawExtractor(transport, storage)

    with pytest.raises(RuntimeError):
        extractor.fetch_and_store(series_id=433, start="01/01/2020", end="31/12/2020")

