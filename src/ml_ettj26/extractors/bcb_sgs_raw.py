from __future__ import annotations

from typing import Optional, Dict, Any, List, Protocol, Tuple
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time

from ml_ettj26.utils.io.http import HttpTransport
from ml_ettj26.utils.io.storage import ByteStorage


# ---------- Range splitting (injeção) ----------

class RangeSplitter(Protocol):
    def split(self, start: str, end: str) -> List[Tuple[str, str]]:
        """Recebe datas dd/mm/aaaa e retorna lista de ranges (start,end) também dd/mm/aaaa."""
        ...


class MaxYearsSplitter:
    """Divide um intervalo em janelas de no máximo N anos (inclusive)."""
    def __init__(self, years: int = 9):
        if years <= 0:
            raise ValueError("years deve ser > 0")
        self.years = years

    @staticmethod
    def _parse(s: str) -> datetime:
        return datetime.strptime(s, "%d/%m/%Y")

    @staticmethod
    def _fmt(dt: datetime) -> str:
        return dt.strftime("%d/%m/%Y")

    def split(self, start: str, end: str) -> List[Tuple[str, str]]:
        start_dt = self._parse(start)
        end_dt = self._parse(end)
        if end_dt < start_dt:
            raise ValueError("end deve ser >= start")

        ranges: List[Tuple[str, str]] = []
        cur_start = start_dt

        while cur_start <= end_dt:
            cur_end = cur_start + relativedelta(years=self.years) - relativedelta(days=1)
            if cur_end > end_dt:
                cur_end = end_dt

            ranges.append((self._fmt(cur_start), self._fmt(cur_end)))
            cur_start = cur_end + relativedelta(days=1)

        return ranges


# ---------- BCB / SGS RAW extractor ----------

class BcbSgsRawExtractor:
    """
    Baixa RAW do SGS e salva o JSON (bytes) exatamente como veio.

    Método público único:
      - fetch_and_store(): sempre retorna List[str] com 1 ou mais arquivos.
    """

    def __init__(
        self,
        transport: HttpTransport,
        storage: ByteStorage,
        splitter: Optional[RangeSplitter] = None,
    ):
        self.transport = transport
        self.storage = storage
        self.splitter = splitter or MaxYearsSplitter(years=9)

    def _fetch_once(
        self,
        series_id: int,
        start: Optional[str] = None,
        end: Optional[str] = None,
        out_path: Optional[str] = None,
    ) -> str:
        
        """
        Faz uma única requisição à API BCB/SGS e salva o resultado.
        
        Responsabilidade: busca + armazenamento (não orquestração).
        Privado porque a orquestração de múltiplas janelas fica em fetch_and_store().
        """

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

    def fetch_and_store(
        self,
        series_id: int,
        start: Optional[str] = None,
        end: Optional[str] = None,
        out_dir: Optional[str] = None,
    ) -> List[str]:
        """
        Baixa e salva o RAW. Se start/end forem informados, aplica chunking via splitter.

        Retorna SEMPRE List[str] (mesmo que só 1 arquivo).
        """
        # Caso sem datas: uma única chamada
        if not start and not end:
            return [self._fetch_once(series_id=series_id, start=None, end=None)]

        # Se um foi passado e o outro não, melhor falhar cedo (evita ambiguidades)
        if not start or not end:
            raise ValueError("Para extrair com período, informe start e end (dd/mm/aaaa).")

        # Divide em ranges e baixa um por um
        paths: List[str] = []
        for s, e in self.splitter.split(start, end):
            chunk_path = None
            if out_dir:
                chunk_path = f"{out_dir.rstrip('/')}/{series_id}_{s.replace('/','-')}_{e.replace('/','-')}.json"
            paths.append(self._fetch_once(series_id=series_id, start=s, end=e, out_path=chunk_path))
            time.sleep(1)

        return paths
