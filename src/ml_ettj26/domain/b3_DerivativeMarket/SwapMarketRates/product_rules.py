from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class SwapProductSpec:
    underlying: Optional[str]
    fixed_leg: Optional[str]
    float_leg: Optional[str]
    calendar: Optional[str]


DEFAULT_SWAP_PRODUCT_SPECS_BY_CODE: dict[str, SwapProductSpec] = {
    # exemplos; ajuste conforme seus códigos reais
    "DI1": SwapProductSpec(
        underlying="DI x PRE",
        fixed_leg="PRE",
        float_leg="DI",
        calendar="BU/252",
    ),
}


DEFAULT_SWAP_PRODUCT_SPECS_BY_NAME: dict[str, SwapProductSpec] = {
    # fallback por nome do produto
    "SWAP DI X PRE": SwapProductSpec(
        underlying="DI x PRE",
        fixed_leg="PRE",
        float_leg="DI",
        calendar="BU/252",
    ),
}


def resolve_swap_product_spec(
    cod_prod: str,
    nome_produto: str,
    specs_by_code: dict[str, SwapProductSpec] | None = None,
    specs_by_name: dict[str, SwapProductSpec] | None = None,
) -> SwapProductSpec:
    specs_by_code = specs_by_code or DEFAULT_SWAP_PRODUCT_SPECS_BY_CODE
    specs_by_name = specs_by_name or DEFAULT_SWAP_PRODUCT_SPECS_BY_NAME

    if cod_prod in specs_by_code:
        return specs_by_code[cod_prod]

    normalized_name = nome_produto.strip().upper()
    if normalized_name in specs_by_name:
        return specs_by_name[normalized_name]

    return SwapProductSpec(
        underlying=None,
        fixed_leg=None,
        float_leg=None,
        calendar=None,
    )
