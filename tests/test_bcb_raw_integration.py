import pytest
from datetime import date

from ml_ettj26.utils.io.http import HTTPConfig, RequestsTransport
from ml_ettj26.utils.io.storage import LocalFileStorage
from ml_ettj26.extractors.bcb_raw import BcbSgsRawExtractor, BcbDemabNegociacoesRawExtractor

import socket
# - teste não quebra caso não tenha internet
def has_internet(host="api.bcb.gov.br", port=443, timeout=2) -> bool:
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except Exception:
        return False


@pytest.mark.integration
def test_integration_bcb_sgs_ipca_downloads_and_saves(tmp_path):
    if not has_internet():
        pytest.skip("Sem conexão com api.bcb.gov.br")

    # Arrange
    transport = RequestsTransport(HTTPConfig(timeout_sec=30, max_retries=2, backoff_sec=0.5))
    storage = LocalFileStorage(str(tmp_path))

    extractor = BcbSgsRawExtractor(transport, storage)

    # Act (um intervalo pequeno pra ficar rápido)
    out_path = extractor.fetch_and_store(series_id=433, start="01/01/2020", end="31/12/2020")

    # Assert
    p = tmp_path / out_path
    assert p.exists()
    content = p.read_bytes()
    assert content.startswith(b"[")  # JSON array


@pytest.mark.integration
def test_integration_bcb_demab_one_month_downloads_zip(tmp_path):
    if not has_internet():
        pytest.skip("Sem conexão com api.bcb.gov.br")

    # Arrange
    transport = RequestsTransport(HTTPConfig(timeout_sec=30, max_retries=2, backoff_sec=0.5))
    storage = LocalFileStorage(str(tmp_path))

    extractor = BcbDemabNegociacoesRawExtractor(transport, storage)

    # Act (um mês específico)
    out_path = extractor.fetch_and_store_month(month=date(2024, 1, 1), tipo="T")

    # Assert
    p = tmp_path / out_path
    assert p.exists()
    content = p.read_bytes()
    # ZIP normalmente começa com PK
    assert content[:2] == b"PK"
