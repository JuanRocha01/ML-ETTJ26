from __future__ import annotations

from dataclasses import dataclass

from .mapper import SwapLineMapper
from .parsing import parse_swap_txt_line
from .models import DataLineage, SwapDIxPRE, SwapMaster


@dataclass(frozen=True)
class SwapTrustedBuildResult:
    swap_dixpre: list[SwapDIxPRE]
    swap_master: list[SwapMaster]
    data_lineage: list[DataLineage]


class SwapTrustedBuilderService:
    def __init__(self, mapper: SwapLineMapper):
        self.mapper = mapper

    def build_from_payload(
        self,
        *,
        outer_zip: str,
        inner_zip: str,
        txt_name: str,
        text: str,
        hash_file: str,
    ) -> SwapTrustedBuildResult:
        lineage = self.mapper.to_data_lineage(
            outer_zip=outer_zip,
            inner_zip=inner_zip,
            txt_name=txt_name,
            hash_file=hash_file,
        )

        lines = [line.rstrip("\n") for line in text.splitlines() if line.strip()]
        raw_records = [parse_swap_txt_line(line) for line in lines]

        swap_rows = [
            self.mapper.to_swap_dixpre(record=record, lineage_id=lineage.lineage_id)
            for record in raw_records
        ]

        master_by_codprod: dict[str, SwapMaster] = {}
        for record in raw_records:
            master_by_codprod[record.codigo_produto] = self.mapper.to_swap_master(record)

        return SwapTrustedBuildResult(
            swap_dixpre=swap_rows,
            swap_master=list(master_by_codprod.values()),
            data_lineage=[lineage],
        )
    