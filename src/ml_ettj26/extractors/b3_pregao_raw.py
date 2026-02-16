from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, List

from ml_ettj26.utils.io.http import HttpTransport
from ml_ettj26.utils.io.storage import ByteStorage
from ml_ettj26.utils.io.paths import project_root

from ml_ettj26.extractors.b3_pregao_specs import B3PregaoDailyDatasetSpec


class DownloadError(RuntimeError):
    pass


@dataclass(frozen=True)
class B3PregaoHttpConfig:
    """
    Config do extractor HTTP.

    - project_root: raiz do projeto (onde fica a pasta 'data/')
      Se None, usa project_root() (Path.cwd()).
    - strict:
        False => dias sem arquivo (feriados/fins de semana) são pulados
        True  => levanta DownloadError
    """
    base_url: str = "https://www.b3.com.br/pesquisapregao/download"
    project_root: Optional[Path] = None
    strict: bool = False

    def raw_root_dir(self) -> Path:
        root = self.project_root or project_root()
        return root / "data" / "01_raw" / "b3"


class B3PregaoDailyRawExtractor:
    """
    Extractor RAW diário (HTTP-only) para a Pesquisa por Pregão da B3.

    Responsabilidade única:
      - baixar bytes via /pesquisapregao/download?filelist=...
      - persistir via ByteStorage
    """

    def __init__(
        self,
        transport: HttpTransport,
        storage: ByteStorage,
        dataset: B3PregaoDailyDatasetSpec,
        cfg: Optional[B3PregaoHttpConfig] = None,
    ):
        self.transport = transport
        self.storage = storage
        self.dataset = dataset
        self.cfg = cfg or B3PregaoHttpConfig()

    @staticmethod
    def _day_floor(d: date) -> date:
        return date(d.year, d.month, d.day)

    def _default_out_path(self, trading_day: date) -> str:
        """
        Salva exatamente em:
          <project_root>/data/01_raw/b3/<DatasetKey>/<nomeOriginal_yyyymmdd>.zip
        """
        filename = self.dataset.build_saved_filename(trading_day)
        full_path = self.cfg.raw_root_dir() / self.dataset.key / filename
        return str(full_path)

    def _fetch_once(self, trading_day: date, out_path: Optional[str]) -> Optional[str]:
        """
        Baixa 1 dia via /pesquisapregao/download?filelist=...

        Regras:
        - 404 => (strict) erro | (non-strict) pula
        - content vazio => (strict) erro | (non-strict) pula
        - content não é ZIP => (strict) erro | (non-strict) pula
        - ZIP válido porém vazio => (strict) erro | (non-strict) pula
        - caso ok => salva
        """
        d = self._day_floor(trading_day)
        filelist = self.dataset.build_filelist_param(d)
        save_path = out_path or self._default_out_path(d)

        try:
            r = self.transport.get(self.cfg.base_url, params={"filelist": filelist})

            # Dia sem arquivo: tipicamente 404 (ou pode ser 200 com payload pequeno em alguns casos).
            if r.status_code == 404:
                if self.cfg.strict:
                    raise DownloadError(
                        f"Arquivo não encontrado: dataset={self.dataset.key} day={d} filelist={filelist}"
                    )
                return None

            r.raise_for_status()

        except Exception as exc:
            raise DownloadError(
                f"Falha ao baixar B3: dataset={self.dataset.key} day={d} filelist={filelist} url={self.cfg.base_url}"
            ) from exc

        content = r.content

        # Resposta vazia
        if not content:
            if self.cfg.strict:
                raise DownloadError(
                    f"Resposta vazia: dataset={self.dataset.key} day={d} filelist={filelist}"
                )
            return None

        # Validação semântica: deve ser ZIP (normalmente começa com PK)
        if not content.startswith(b"PK"):
            if self.cfg.strict:
                raise DownloadError(
                    f"Resposta não é ZIP: dataset={self.dataset.key} day={d} filelist={filelist}"
                )
            return None

        # Validação semântica: ZIP não pode estar vazio
        import io
        import zipfile

        try:
            with zipfile.ZipFile(io.BytesIO(content), "r") as zf:
                # ignora diretórios (entradas terminadas em "/")
                names = [n for n in zf.namelist() if n and not n.endswith("/")]
                if len(names) == 0:
                    if self.cfg.strict:
                        raise DownloadError(
                            f"ZIP vazio: dataset={self.dataset.key} day={d} filelist={filelist}"
                        )
                    return None
        except zipfile.BadZipFile:
            if self.cfg.strict:
                raise DownloadError(
                    f"Conteúdo inválido (BadZipFile): dataset={self.dataset.key} day={d} filelist={filelist}"
                )
            return None

        # ByteStorage deve criar diretórios; se não criar, ajuste o storage (ideal).
        return self.storage.save(save_path, content)

    def fetch_and_store(
        self,
        *,
        day: Optional[date] = None,
        start_day: Optional[date] = None,
        end_day: Optional[date] = None,
        out_dir: Optional[str] = None,
    ) -> List[str]:
        """
        Único entry-point:
        - day: baixa um dia
        - start_day/end_day: baixa intervalo diário inclusivo
        - strict=False: dias sem arquivo são pulados (retorna lista menor)
        """
        if day is not None:
            if start_day is not None or end_day is not None:
                raise ValueError("Use OU day OU (start_day e end_day), não ambos.")

            d = self._day_floor(day)
            out_path = None
            if out_dir:
                filename = self.dataset.build_saved_filename(d)
                out_path = str(Path(out_dir) / self.dataset.key / filename)

            p = self._fetch_once(trading_day=d, out_path=out_path)
            return [p] if p else []

        if (start_day is None) != (end_day is None):
            raise ValueError("Para intervalo, informe start_day e end_day juntos.")

        if start_day is None and end_day is None:
            raise ValueError("Informe day OU (start_day e end_day).")

        s = self._day_floor(start_day)  # type: ignore[arg-type]
        e = self._day_floor(end_day)    # type: ignore[arg-type]
        if e < s:
            raise ValueError("end_day deve ser >= start_day.")

        paths: List[str] = []
        cur = s
        while cur <= e:
            out_path = None
            if out_dir:
                filename = self.dataset.build_saved_filename(cur)
                out_path = str(Path(out_dir) / self.dataset.key / filename)

            p = self._fetch_once(trading_day=cur, out_path=out_path)
            if p:
                paths.append(p)
            cur = cur + timedelta(days=1)

        return paths
