from __future__ import annotations

from ml_ettj26.domain.b3_DerivativeMarket.SwapMarketRates.helpers import make_lineage_id
from ml_ettj26.domain.b3_DerivativeMarket.SwapMarketRates.mapper import SwapLineMapper


def test_make_lineage_id_returns_deterministic_sha256_hex():
    lineage_id = make_lineage_id(
        outer_zip="TS250101_20250101.zip",
        inner_zip="TS250101.ex_",
        txt_name="TaxaSwap.txt",
        hash_file="abc123",
    )

    assert lineage_id == "0345c5251ac1ee5c0b3be9b52abcc5476f013743df37d89933429ddd129c428e"
    assert len(lineage_id) == 64
    assert all(char in "0123456789abcdef" for char in lineage_id)


def test_mapper_data_lineage_uses_hexadecimal_lineage_id():
    lineage = SwapLineMapper().to_data_lineage(
        outer_zip="TS250101_20250101.zip",
        inner_zip="TS250101.ex_",
        txt_name="TaxaSwap.txt",
        hash_file="abc123",
    )

    assert lineage.lineage_id == "0345c5251ac1ee5c0b3be9b52abcc5476f013743df37d89933429ddd129c428e"
