from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from typing import Dict, Tuple

from ml_ettj26.utils.io.http import HttpTransport  # ajuste o import


# Esses códigos são “chaves” de download do boletim.
# Vamos mantê-los isolados para facilitar manutenção.
B3_FILE_CODES: Dict[str, Tuple[str, str]] = {
    "swap_market_rates": ("C8", "T8"),              # Taxas de Mercado para Swaps
    "settlement_adjustments": ("C3", "T3"),         # Ajustes (derivativos)
}

@dataclass(frozen=True)
class B3DownloadConfig:
    base_url: str = "https://www.bmf.com.br/arquivos1/download.asp"

class B3BoletimDownloader:
    def __init__(self, transport: HttpTransport, cfg: B3DownloadConfig = B3DownloadConfig()):
        self.transport = transport
        self.cfg = cfg

    @staticmethod
    def _fmt_ref_date(d: date) -> str:
        return d.strftime("%d/%m/%Y")

    def download_zip(self, file_key: str, ref_date: date) -> bytes:
        if file_key not in B3_FILE_CODES:
            raise ValueError(f"file_key inválida: {file_key}")

        c_code, t_code = B3_FILE_CODES[file_key]
        data = {c_code: "ON", t_code: self._fmt_ref_date(ref_date)}

        r = self.transport.post(self.cfg.base_url, data=data)
        r.raise_for_status()
        return r.content
