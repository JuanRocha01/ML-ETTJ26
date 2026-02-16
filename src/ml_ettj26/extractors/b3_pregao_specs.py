from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal


B3PregaoDatasetKey = Literal["PriceReport", "DerivativesMarket-SwapMarketRates"]


@dataclass(frozen=True)
class B3PregaoDailyDatasetSpec:
    """
    Spec do dataset diário da B3 Pesquisa por Pregão.

    Responsabilidade:
      - construir o código (filelist) esperado pelo endpoint
      - definir um nome estável para salvar localmente
    """

    key: B3PregaoDatasetKey
    prefix: str  # "PR" (PriceReport) ou "TS" (Swap Market Rates)
    ext: str     # "zip" (PR) ou "ex_" (TS)
    description: str

    def build_file_code(self, trading_date: date) -> str:
        """
        Código usado pela B3 no parâmetro filelist (sem vírgula):
          PR + yymmdd + .zip -> PR210302.zip
          TS + yymmdd + .ex_ -> TS200213.ex_
        """
        yymmdd = trading_date.strftime("%y%m%d")
        return f"{self.prefix}{yymmdd}.{self.ext}"

    def build_filelist_param(self, trading_date: date) -> str:
        """
        O endpoint espera 'filelist' terminando em vírgula.
        """
        return f"{self.build_file_code(trading_date)},"

    def build_saved_filename(self, trading_date: date) -> str:
        """
        Você pediu: "nomeOriginal_yyyymmdd."
        Como o servidor retorna um ZIP (filename="pesquisa-pregao.zip"),
        salvamos sempre como .zip, usando o stem do "nome original lógico".
        """
        ymd = trading_date.strftime("%Y%m%d")
        original_code = self.build_file_code(trading_date)  # PR210302.zip | TS200213.ex_
        original_stem = original_code.split(".")[0]         # PR210302 | TS200213
        return f"{original_stem}_{ymd}.zip"


PRICE_REPORT = B3PregaoDailyDatasetSpec(
    key="PriceReport",
    prefix="PR",
    ext="zip",
    description="BVBG.086.01 PriceReport (Boletim de Negociação)",
)

SWAP_MARKET_RATES = B3PregaoDailyDatasetSpec(
    key="DerivativesMarket-SwapMarketRates",
    prefix="TS",
    ext="ex_",
    description="Derivatives Market - Swap Market Rates",
)
