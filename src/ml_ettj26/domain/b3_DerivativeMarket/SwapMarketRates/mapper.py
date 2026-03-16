from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .models import DataLineage, SwapDIxPRE, SwapMaster
from .product_rules import SwapProductSpec, resolve_swap_product_spec
from .parsing import SwapRawRecord
from .helpers import (
    adjusted_value_from_raw,
    make_lineage_id,
    parse_int,
    parse_yyyymmdd,
    utcnow,
)


@dataclass(frozen=True)
class SwapLineMapperConfig:
    adjusted_value_scale: int = 10_000


class SwapLineMapper:
    def __init__(
        self,
        config: Optional[SwapLineMapperConfig] = None,
        specs_by_code: dict[str, SwapProductSpec] | None = None,
        specs_by_name: dict[str, SwapProductSpec] | None = None,
    ):
        self.config = config or SwapLineMapperConfig()
        self.specs_by_code = specs_by_code
        self.specs_by_name = specs_by_name

    def to_data_lineage(
        self,
        outer_zip: str,
        inner_zip: str,
        txt_name: str,
        hash_file: str,
    ) -> DataLineage:
        lineage_id = make_lineage_id(
            outer_zip=outer_zip,
            inner_zip=inner_zip,
            txt_name=txt_name,
            hash_file=hash_file,
        )
        return DataLineage(
            lineage_id=lineage_id,
            outer_zip=outer_zip,
            inner_zip=inner_zip,
            txt_name=txt_name,
            hash_file=hash_file,
        )

    def to_swap_dixpre(
        self,
        record: SwapRawRecord,
        lineage_id: str,
    ) -> SwapDIxPRE:
        return SwapDIxPRE(
            TradDt=parse_yyyymmdd(record.data_ref_raw),
            CodProd=record.codigo_produto,
            days_to_maturity=parse_int(record.dias_corridos_maturity),
            bd_to_maturity=parse_int(record.dias_uteis),
            days_to_delivery=parse_int(record.dias_entrega),
            raw_value=parse_int(record.valor_raw),
            raw_signal=record.sinal,
            adjusted_value=adjusted_value_from_raw(
                raw_value=record.valor_raw,
                signal=record.sinal,
                scale=self.config.adjusted_value_scale,
            ),
            tipo_cotacao=record.tipo_cotacao,
            lineage_id=lineage_id,
            ingestion_ts_utc=utcnow(),
        )

    def to_swap_master(self, record: SwapRawRecord) -> SwapMaster:
        spec = resolve_swap_product_spec(
            cod_prod=record.codigo_produto,
            nome_produto=record.nome_produto,
            specs_by_code=self.specs_by_code,
            specs_by_name=self.specs_by_name,
        )

        return SwapMaster(
            CodProd=record.codigo_produto,
            nome=record.nome_produto,
            underlying=spec.underlying,
            fixed_leg=spec.fixed_leg,
            float_leg=spec.float_leg,
            calendar=spec.calendar,
        )