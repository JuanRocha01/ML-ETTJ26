from __future__ import annotations

from typing import Optional, Dict, Any
from datetime import date

from ml_ettj26.utils.io.http import HttpTransport
from ml_ettj26.utils.io.storage import ByteStorage


class BcbSgsRawExtractor:
    def __init__(self, transport: HttpTransport, storage: ByteStorage):
        self.transport = transport
        self.storage = storage

    def fetch_and_store(
        self,
        series_id: int,
        start: Optional[str] = None,  # dd/mm/aaaa
        end: Optional[str] = None,    # dd/mm/aaaa
        out_path: Optional[str] = None,
    ) -> str:
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_id}/dados"
        params: Dict[str, Any] = {"formato": "json"}
        if start:
            params["dataInicial"] = start
        if end:
            params["dataFinal"] = end

        r = self.transport.get(url, params=params)
        r.raise_for_status()

        if out_path is None:
            start_tag = (start or "NA").replace("/", "-")
            end_tag = (end or "NA").replace("/", "-")
            out_path = f"bcb/sgs/{series_id}_{start_tag}_{end_tag}.json"

        return self.storage.save(out_path, r.content)


class BcbDemabNegociacoesRawExtractor:
    def __init__(self, transport: HttpTransport, storage: ByteStorage):
        self.transport = transport
        self.storage = storage

    @staticmethod
    def _yyyymm(d: date) -> str:
        return f"{d.year:04d}{d.month:02d}"

    def fetch_and_store_month(
        self,
        month: date,
        tipo: str = "T",   # "T" todas; "E" extragrupo
        out_path: Optional[str] = None,
    ) -> str:
        tipo = tipo.upper()
        if tipo not in ("T", "E"):
            raise ValueError("tipo deve ser 'T' ou 'E'.")

        yyyymm = self._yyyymm(month)
        url = f"https://www4.bcb.gov.br/pom/demab/negociacoes/download/Neg{tipo}{yyyymm}.ZIP"

        r = self.transport.get(url)
        r.raise_for_status()

        if out_path is None:
            out_path = f"bcb/demab/negociacoes/Neg{tipo}{yyyymm}.ZIP"

        return self.storage.save(out_path, r.content)
