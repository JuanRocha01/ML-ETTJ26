from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

DemabTipo = Literal["T", "E"]  # T=todas, E=extragrupo


@dataclass(frozen=True)
class DemabMonthlyDatasetSpec:
    """
    Especificação de um dataset DEMAB que é disponibilizado por arquivo mensal.

    - url_template: template relativo (sem base_url), usando {tipo} e {yyyymm}
    - filename_template: template para nome do arquivo salvo, também usando {tipo} e {yyyymm}
    """
    key: str
    url_template: str
    filename_template: str

    def build_relative_url(self, tipo: DemabTipo, yyyymm: str) -> str:
        return self.url_template.format(tipo=tipo, yyyymm=yyyymm)

    def build_filename(self, tipo: DemabTipo, yyyymm: str) -> str:
        return self.filename_template.format(tipo=tipo, yyyymm=yyyymm)


# Dataset DEMAB já conhecido: Negociação de Títulos Federais no Mercado Secundário
DEMAB_NEGOCIACOES = DemabMonthlyDatasetSpec(
    key="negociacoes_titulos_federais_secundario",
    url_template="negociacoes/download/Neg{tipo}{yyyymm}.ZIP",
    filename_template="Neg{tipo}{yyyymm}.ZIP",
)
