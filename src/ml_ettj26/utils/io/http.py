from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any, Protocol
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class HttpTransport(Protocol):
    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response: ...


@dataclass(frozen=True)
class HTTPConfig:
    timeout_sec: int = 30
    max_retries: int = 4
    backoff_sec: float = 1.0
    headers: Optional[Dict[str, str]] = None


class RequestsTransport:
    def __init__(self, cfg: HTTPConfig):
        self.cfg = cfg
        session = requests.Session()
        retries = Retry(
            total=cfg.max_retries,
            backoff_factor=cfg.backoff_sec,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        self.session = session

    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        h = headers or self.cfg.headers
        return self.session.get(url, params=params, headers=h, timeout=self.cfg.timeout_sec)
