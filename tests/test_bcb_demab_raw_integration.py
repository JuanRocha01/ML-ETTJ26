import pytest
from datetime import date
import socket

from ml_ettj26.utils.io.http import HTTPConfig, RequestsTransport
from ml_ettj26.utils.io.storage import LocalFileStorage
from ml_ettj26.extractors.bcb_demab_specs import DEMAB_NEGOCIACOES
from ml_ettj26.extractors.bcb_demab_raw import DemabMonthlyRawExtractor, DemabConfig


def has_internet(host: str = "www4.bcb.gov.br", port: int = 443, timeout: int = 2) -> bool:
    """
    Verifica rapidamente se há conectividade TCP com o host do BCB.
    Se não houver, pulamos os testes de integração (evita falso 'fail' sem internet).
    """
    try:
        socket.setdefaulttimeout(timeout)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
        return True
    except OSError:
        return False


@pytest.mark.integration
def test_integration_demab_single_month_downloads_zip_and_saves(tmp_path):
    """
    Propósito:
    - Verificar que a URL real do DEMAB funciona (sem mocks)
    - Verificar que o conteúdo é ZIP (começa com 'PK' e tem tamanho plausível)
    - Verificar que o arquivo foi salvo no storage (tmp_path)
    """
    if not has_internet():
        pytest.skip("Sem conexão com www4.bcb.gov.br")

    transport = RequestsTransport(HTTPConfig(timeout_sec=60, max_retries=2, backoff_sec=0.5))
    storage = LocalFileStorage(str(tmp_path))

    extractor = DemabMonthlyRawExtractor(
        transport=transport,
        storage=storage,
        dataset=DEMAB_NEGOCIACOES,
        cfg=DemabConfig(base_url="https://www4.bcb.gov.br/pom/demab", default_out_dir="bcb/demab"),
    )

    paths = extractor.fetch_and_store(tipo="T", month=date(2024, 1, 1), out_dir="bcb/demab")
    assert len(paths) == 1

    saved_path = tmp_path / paths[0]
    assert saved_path.exists()

    content = saved_path.read_bytes()
    assert content[:2] == b"PK"     # ZIP signature
    assert len(content) > 1000      # evita falso positivo com HTML curto


@pytest.mark.integration
def test_integration_demab_extragrupo_single_month_downloads_zip(tmp_path):
    """
    Propósito:
    - Validar especificamente o modo 'E' (extragrupo) em um mês real.
    """
    if not has_internet():
        pytest.skip("Sem conexão com www4.bcb.gov.br")

    transport = RequestsTransport(HTTPConfig(timeout_sec=60, max_retries=2, backoff_sec=0.5))
    storage = LocalFileStorage(str(tmp_path))

    extractor = DemabMonthlyRawExtractor(
        transport=transport,
        storage=storage,
        dataset=DEMAB_NEGOCIACOES,
        cfg=DemabConfig(base_url="https://www4.bcb.gov.br/pom/demab", default_out_dir="bcb/demab"),
    )

    paths = extractor.fetch_and_store(tipo="E", month=date(2024, 1, 1), out_dir="bcb/demab")
    assert len(paths) == 1

    saved_path = tmp_path / paths[0]
    assert saved_path.exists()

    content = saved_path.read_bytes()
    assert content[:2] == b"PK"
    assert len(content) > 1000


@pytest.mark.integration
def test_integration_demab_range_downloads_multiple_months(tmp_path):
    """
    Propósito:
    - Validar o fluxo de intervalo (loop mensal):
      start_month..end_month => N downloads => N arquivos salvos
    - Mantém intervalo curto para ficar rápido.
    """
    if not has_internet():
        pytest.skip("Sem conexão com www4.bcb.gov.br")

    transport = RequestsTransport(HTTPConfig(timeout_sec=60, max_retries=2, backoff_sec=0.5))
    storage = LocalFileStorage(str(tmp_path))

    extractor = DemabMonthlyRawExtractor(
        transport=transport,
        storage=storage,
        dataset=DEMAB_NEGOCIACOES,
        cfg=DemabConfig(base_url="https://www4.bcb.gov.br/pom/demab", default_out_dir="bcb/demab"),
    )

    paths = extractor.fetch_and_store(
        tipo="T",
        start_month=date(2024, 1, 1),
        end_month=date(2024, 2, 1),
        out_dir="bcb/demab",
    )

    assert len(paths) == 2

    for p in paths:
        saved_path = tmp_path / p
        assert saved_path.exists()
        content = saved_path.read_bytes()
        assert content[:2] == b"PK"
        assert len(content) > 1000
