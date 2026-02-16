from datetime import date
from pathlib import Path
import zipfile

import pytest

from ml_ettj26.utils.io.http import HTTPConfig, RequestsTransport
from ml_ettj26.utils.io.storage import LocalFileStorage

from ml_ettj26.extractors.b3_pregao_specs import PRICE_REPORT, SWAP_MARKET_RATES
from ml_ettj26.extractors.b3_pregao_raw import (
    B3PregaoDailyRawExtractor,
    B3PregaoHttpConfig,
    DownloadError,
)


def _internet_available() -> bool:
    """
    Checagem simples de conectividade.
    NÃO usa timeout custom no get().
    """
    try:
        transport = RequestsTransport(HTTPConfig(timeout_sec=10))
        r = transport.get("https://www.b3.com.br/")
        return 200 <= r.status_code < 500
    except Exception:
        return False


@pytest.mark.integration
def test_integration_b3_swap_download(tmp_path):
    if not _internet_available():
        pytest.skip("Sem acesso à internet ou B3 indisponível.")

    transport = RequestsTransport(
        HTTPConfig(timeout_sec=30, max_retries=2, backoff_sec=0.5)
    )
    storage = LocalFileStorage(str(tmp_path))

    cfg = B3PregaoHttpConfig(
        project_root=tmp_path,
        strict=True,
    )

    extractor = B3PregaoDailyRawExtractor(
        transport, storage, SWAP_MARKET_RATES, cfg
    )

    d = date(2020, 2, 13)  # data já validada manualmente
    paths = extractor.fetch_and_store(day=d)

    assert len(paths) == 1

    saved = tmp_path / paths[0]
    assert saved.exists()
    assert saved.stat().st_size > 0

    # Valida se é ZIP válido
    with zipfile.ZipFile(saved, "r") as zf:
        assert len(zf.namelist()) > 0


@pytest.mark.integration
def test_integration_b3_price_report_download(tmp_path):
    if not _internet_available():
        pytest.skip("Sem acesso à internet ou B3 indisponível.")

    transport = RequestsTransport(
        HTTPConfig(timeout_sec=30, max_retries=2, backoff_sec=0.5)
    )
    storage = LocalFileStorage(str(tmp_path))

    cfg = B3PregaoHttpConfig(
        project_root=tmp_path,
        strict=True,
    )

    extractor = B3PregaoDailyRawExtractor(
        transport, storage, PRICE_REPORT, cfg
    )

    d = date(2021, 3, 2)
    paths = extractor.fetch_and_store(day=d)

    assert len(paths) == 1

    saved = tmp_path / paths[0]
    assert saved.exists()
    assert saved.stat().st_size > 0

    with zipfile.ZipFile(saved, "r") as zf:
        assert len(zf.namelist()) > 0


@pytest.mark.integration
def test_integration_b3_missing_day_strict_true_raises(tmp_path):
    if not _internet_available():
        pytest.skip("Sem acesso à internet ou B3 indisponível.")

    transport = RequestsTransport(
        HTTPConfig(timeout_sec=30, max_retries=2, backoff_sec=0.5)
    )
    storage = LocalFileStorage(str(tmp_path))

    cfg = B3PregaoHttpConfig(
        project_root=tmp_path,
        strict=True,
    )

    extractor = B3PregaoDailyRawExtractor(
        transport, storage, SWAP_MARKET_RATES, cfg
    )

    # Domingo (normalmente inexistente)
    d = date(2020, 2, 16)

    with pytest.raises(DownloadError):
        extractor.fetch_and_store(day=d)
