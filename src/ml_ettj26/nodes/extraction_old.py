from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Dict, Any, List

import io
import time
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class HTTPConfig:
    timeout_sec: int = 30
    max_retries: int = 4
    backoff_sec: float = 1.0
    headers: Optional[Dict[str, str]] = None

class MarketDataClient:
    """
    Ciente para baixar dados de mercado via HTTP com retry/backoff.
    Uso com Kedro nodes que chamam esse client.
    """

    def __init__(self, http: HTTPConfig, transport: Optional["HttpTransport"] = None):
        self.http = http
        # Permite injeção de um transporte HTTP para facilitar testes/substituições
        self.transport = transport or HttpTransport(http)

    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Delegates a requisição ao `HttpTransport` configurado."""
        try:
            r = self.transport.get(url, params=params, headers=self.http.headers)
            r.raise_for_status()
            return r
        except Exception as exc:
            logger.exception("Falha ao baixar %s: %s", url, exc)
            raise DownloadError(f"Falha ao baixar dados de {url}") from exc
    
    # ---------- BCB / SGS (JSON) ----------
    def fetch_bcb_sgs_series(
        self,
        series_id: int,
        start: Optional[str] = None,  # "dd/mm/yyyy"
        end: Optional[str] = None,    # "dd/mm/yyyy"
    ) -> pd.DataFrame:
        """
        API oficial BCB SGS:
        https://api.bcb.gov.br/dados/serie/bcdata.sgs.{id}/dados?formato=json
        + opcional: dataInicial, dataFinal (dd/mm/aaaa)
        """
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados"
        params = {"formato": "json"}
        if start:
            params["dataInicial"] = start
        if end:
            params["dataFinal"] = end

        r = self._get(url, params=params)
        df = pd.read_json(io.BytesIO(r.content))
        # padroniza colunas
        df = df.rename(columns={"data": "date", "valor": "value"})
        df["date"] = pd.to_datetime(df["date"], dayfirst=True)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.sort_values("date").reset_index(drop=True)
        return df


class DownloadError(RuntimeError):
    pass


class HttpTransport:
    """Transporte HTTP configurável com retry, baseado em `requests.Session`.

    Usa `urllib3.Retry` via `HTTPAdapter` para lidar com backoff/exponential retry
    no nível do adaptador em vez de retry manual.
    """

    def __init__(self, cfg: HTTPConfig, session: Optional[requests.Session] = None):
        self.cfg = cfg
        self.session = session or requests.Session()
        retries = Retry(
            total=cfg.max_retries,
            backoff_factor=cfg.backoff_sec,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST"),
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get(self, url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> requests.Response:
        return self.session.get(url, params=params, headers=headers, timeout=self.cfg.timeout_sec)


class BcbClient:
    """Cliente específico para BCB/SGS usando um transporte HTTP injetável.

    Extrai a lógica de parsing fora do `MarketDataClient` para respeitar
    separação de responsabilidades.
    """

    def __init__(self, transport: HttpTransport):
        self.transport = transport

    def fetch_series(self, series_id: int, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados"
        params = {"formato": "json"}
        if start:
            params["dataInicial"] = start
        if end:
            params["dataFinal"] = end

        r = self.transport.get(url, params=params)
        r.raise_for_status()
        df = pd.read_json(io.BytesIO(r.content))
        df = df.rename(columns={"data": "date", "valor": "value"})
        df["date"] = pd.to_datetime(df["date"], dayfirst=True)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.sort_values("date").reset_index(drop=True)
        return df

    # ---------- ANBIMA (texto/CSV) ----------
    def fetch_anbima_text(self, url: str, sep: str = ";", encoding: str = "latin-1") -> pd.DataFrame:
        """
        Muitas páginas/arquivos da ANBIMA são TXT/CSV com separador ';'.
        Aqui a URL é passada pronta via parameters.yml (você controla o padrão).
        """
        r = self._get(url)
        content = r.content.decode(encoding, errors="replace")
        df = pd.read_csv(io.StringIO(content), sep=sep)
        return df

    # ---------- B3 (arquivos / dumps) ----------
    def fetch_b3_file(self, url: str, encoding: str = "latin-1") -> bytes:
        """
        Para B3, com frequência você baixa um arquivo (csv/txt/zip).
        Aqui devolvemos bytes e você faz o parser conforme o layout.
        """
        r = self._get(url)
        return r.content


# ---------------- Kedro nodes ----------------

def extract_bcb_rates(params: dict) -> Dict[str, pd.DataFrame]:
    """
    Baixa séries SGS do BCB (ex.: Selic, IPCA, etc.) conforme params.
    Retorna dict[name] = DataFrame(date,value).
    """
    http_cfg = HTTPConfig(
        timeout_sec=params.get("timeout_sec", 30),
        max_retries=params.get("max_retries", 4),
        backoff_sec=params.get("backoff_sec", 1.0),
        headers=params.get("headers"),
    )
    # Cria transporte e cliente específico para BCB/SGS (mais testável e SRP)
    transport = HttpTransport(http_cfg)
    bcb = BcbClient(transport)

    start = params.get("start")  # "dd/mm/yyyy"
    end = params.get("end")      # "dd/mm/yyyy"

    series: Dict[str, int] = params["series"]  # {"selic": 432, "ipca": 433, ...}
    out: Dict[str, pd.DataFrame] = {}

    for name, sid in series.items():
        df = bcb.fetch_series(series_id=int(sid), start=start, end=end)
        df["series_name"] = name
        df["series_id"] = int(sid)
        out[name] = df

    return out


def extract_anbima_bonds(params: dict) -> pd.DataFrame:
    """
    Baixa o arquivo diário/histórico da ANBIMA.
    Você passa a URL pronta (com data interpolada) via parameters.yml.
    """
    http_cfg = HTTPConfig(
        timeout_sec=params.get("timeout_sec", 30),
        max_retries=params.get("max_retries", 4),
        backoff_sec=params.get("backoff_sec", 1.0),
        headers=params.get("headers"),
    )
    client = MarketDataClient(http_cfg)

    url = params["url"]  # ex: já com {YYYYMMDD} resolvido no node anterior ou em params
    sep = params.get("sep", ";")
    encoding = params.get("encoding", "latin-1")

    df = client.fetch_anbima_text(url=url, sep=sep, encoding=encoding)
    # aqui você pode padronizar nomes depois num node de cleaning
    df["source"] = "ANBIMA"
    return df


def extract_b3_di_futures(params: dict) -> pd.DataFrame:
    """
    Baixa arquivo(s) da B3 com dados de DI Futuro conforme URL que você fornecer.
    Como a B3 tem múltiplos layouts, aqui deixo o parser configurável.
    """
    http_cfg = HTTPConfig(
        timeout_sec=params.get("timeout_sec", 30),
        max_retries=params.get("max_retries", 4),
        backoff_sec=params.get("backoff_sec", 1.0),
        headers=params.get("headers"),
    )
    client = MarketDataClient(http_cfg)

    url = params["url"]          # URL do arquivo
    file_type = params.get("file_type", "csv")  # "csv" ou "txt"
    sep = params.get("sep", ";")
    encoding = params.get("encoding", "latin-1")

    raw = client.fetch_b3_file(url=url, encoding=encoding)

    # Parser genérico (ajuste conforme o layout do arquivo que você escolher)
    text = raw.decode(encoding, errors="replace")
    df = pd.read_csv(io.StringIO(text), sep=sep)
    df["source"] = "B3"
    return df





