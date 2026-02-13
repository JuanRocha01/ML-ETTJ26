import pytest
from datetime import date

from ml_ettj26.utils.io.http import HTTPConfig, RequestsTransport
from ml_ettj26.utils.io.storage import LocalFileStorage
from ml_ettj26.extractors.bcb_raw import BcbSgsRawExtractor, BcbDemabNegociacoesRawExtractor


@pytest.mark.integration
def test_integration_bcb_sgs_ipca_downloads_and_saves(tmp_path):
    # Arrange
    transport = RequestsTransport(HTTPConfig(timeout_sec=30, max_retries=2, backoff_sec=0.5))
    storage = LocalFileStorage(str(tmp_path))
    extractor = BcbSgsRawExtractor(transport, storage)

    # Act (range curto => deve gerar 1 arquivo)
    paths = extractor.fetch_and_store(series_id=433, start="01/01/2020", end="31/12/2020")

    # Assert
    assert len(paths) >= 1
    p = tmp_path / paths[0]
    assert p.exists()

    content = p.read_bytes()
    # Resposta JSON do SGS normalmente é um array
    assert content.lstrip().startswith(b"[")


@pytest.mark.integration
def test_integration_bcb_sgs_selic_long_range_creates_multiple_files(tmp_path):
    """
    Teste de integração que valida o chunking real (>= 2 arquivos).
    Mantemos um range > 10 anos para forçar múltiplas chamadas.
    """
    # Arrange
    transport = RequestsTransport(HTTPConfig(timeout_sec=30, max_retries=2, backoff_sec=0.5))
    storage = LocalFileStorage(str(tmp_path))
    extractor = BcbSgsRawExtractor(transport, storage)

    # Act (força chunking; 2011..2026 > 10 anos)
    paths = extractor.fetch_and_store(series_id=432, start="01/01/2011", end="11/02/2026", out_dir="bcb/sgs")

    # Assert
    assert len(paths) >= 2  # chunking aconteceu
    for rel in paths:
        p = tmp_path / rel
        assert p.exists()
        content = p.read_bytes()
        assert content.lstrip().startswith(b"[")


@pytest.mark.integration
def test_integration_bcb_demab_one_month_downloads_zip(tmp_path):
    # Arrange
    transport = RequestsTransport(HTTPConfig(timeout_sec=30, max_retries=2, backoff_sec=0.5))
    storage = LocalFileStorage(str(tmp_path))
    extractor = BcbDemabNegociacoesRawExtractor(transport, storage)

    # Act
    out_path = extractor.fetch_and_store_month(month=date(2024, 1, 1), tipo="T")

    # Assert
    p = tmp_path / out_path
    assert p.exists()
    content = p.read_bytes()
    # ZIP começa com PK
    assert content[:2] == b"PK"
    assert len(content) > 1000  # evita falso positivo com HTML curto
