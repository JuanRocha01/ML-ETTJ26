from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Optional, List

from dateutil.relativedelta import relativedelta

from ml_ettj26.utils.io.http import HttpTransport
from ml_ettj26.utils.io.storage import ByteStorage
from ml_ettj26.extractors.bcb_demab_specs import DemabMonthlyDatasetSpec, DemabTipo


class DownloadError(RuntimeError):
    pass


@dataclass(frozen=True)
class DemabConfig:
    base_url: str = "https://www4.bcb.gov.br/pom/demab"
    default_out_dir: str = "bcb/demab"


class DemabMonthlyRawExtractor:
    """
    Extractor RAW genérico para datasets mensais do DEMAB.

    Único método público:
      - fetch_and_store(...): aceita month OU (start_month, end_month) e retorna List[str].
    """

    def __init__(
        self,
        transport: HttpTransport,
        storage: ByteStorage,
        dataset: DemabMonthlyDatasetSpec,
        cfg: Optional[DemabConfig] = None,
    ):
        self.transport = transport
        self.storage = storage
        self.dataset = dataset
        self.cfg = cfg or DemabConfig()

    @staticmethod
    def _yyyymm(d: date) -> str:
        return f"{d.year:04d}{d.month:02d}"

    @staticmethod
    def _month_floor(d: date) -> date:
        """Normaliza qualquer date para o 1º dia do mês (evita ambiguidade)."""
        return date(d.year, d.month, 1)

    def _validate_tipo(self, tipo: str) -> DemabTipo:
        t = tipo.upper()
        if t not in ("T", "E"):
            raise ValueError(f"tipo inválido: {tipo!r}. Use 'T' (todas) ou 'E' (extragrupo).")
        return t  # type: ignore[return-value]

    def _build_url(self, tipo: DemabTipo, yyyymm: str) -> str:
        rel = self.dataset.build_relative_url(tipo=tipo, yyyymm=yyyymm).lstrip("/")
        return f"{self.cfg.base_url.rstrip('/')}/{rel}"

    def _default_out_path(self, tipo: DemabTipo, yyyymm: str) -> str:
        filename = self.dataset.build_filename(tipo=tipo, yyyymm=yyyymm)
        return f"{self.cfg.default_out_dir.rstrip('/')}/{self.dataset.key}/{filename}"

    def _fetch_once(self, month: date, tipo: DemabTipo, out_path: Optional[str]) -> str:
        m = self._month_floor(month)
        yyyymm = self._yyyymm(m)

        url = self._build_url(tipo=tipo, yyyymm=yyyymm)
        save_path = out_path or self._default_out_path(tipo=tipo, yyyymm=yyyymm)

        try:
            r = self.transport.get(url)
            r.raise_for_status()
        except Exception as exc:
            raise DownloadError(
                f"Falha ao baixar DEMAB '{self.dataset.key}' tipo={tipo} mês={yyyymm} url={url}"
            ) from exc

        return self.storage.save(save_path, r.content)

    def fetch_and_store(
        self,
        *,
        tipo: str = "T",
        month: Optional[date] = None,
        start_month: Optional[date] = None,
        end_month: Optional[date] = None,
        out_dir: Optional[str] = None,
    ) -> List[str]:
        """
        Único entry-point:

        - Se month for informado: baixa 1 mês.
        - Se start_month e end_month forem informados: baixa intervalo mensal inclusivo.
        - Retorna SEMPRE List[str].

        out_dir (opcional):
          - se fornecido, força o diretório raiz para os arquivos desta chamada:
            out_dir/<dataset.key>/<filename>
        """
        t = self._validate_tipo(tipo)

        # Valida combinações de parâmetros
        if month is not None:
            if start_month is not None or end_month is not None:
                raise ValueError("Use OU month OU (start_month e end_month), não ambos.")
            m = self._month_floor(month)
            out_path = None
            if out_dir:
                yyyymm = self._yyyymm(m)
                filename = self.dataset.build_filename(tipo=t, yyyymm=yyyymm)
                out_path = f"{out_dir.rstrip('/')}/{self.dataset.key}/{filename}"
            return [self._fetch_once(month=m, tipo=t, out_path=out_path)]

        # Caso intervalo
        if (start_month is None) != (end_month is None):
            raise ValueError("Para intervalo, informe start_month e end_month juntos.")

        if start_month is None and end_month is None:
            raise ValueError("Informe month OU (start_month e end_month).")

        s = self._month_floor(start_month)  # type: ignore[arg-type]
        e = self._month_floor(end_month)    # type: ignore[arg-type]
        if e < s:
            raise ValueError("end_month deve ser >= start_month.")

        paths: List[str] = []
        cur = s
        while cur <= e:
            out_path = None
            if out_dir:
                yyyymm = self._yyyymm(cur)
                filename = self.dataset.build_filename(tipo=t, yyyymm=yyyymm)
                out_path = f"{out_dir.rstrip('/')}/{self.dataset.key}/{filename}"

            paths.append(self._fetch_once(month=cur, tipo=t, out_path=out_path))
            cur = cur + relativedelta(months=1)

        return paths
