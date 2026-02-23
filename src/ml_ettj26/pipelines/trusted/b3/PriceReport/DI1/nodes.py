from __future__ import annotations

from typing import Dict, Tuple
from datetime import date
import pandas as pd

from ml_ettj26.domain.b3_PriceReport.service import build_b3_di1_trusted_month


def _iter_months(start_ym: str, end_ym: str):
    sy, sm = map(int, start_ym.split("-"))
    ey, em = map(int, end_ym.split("-"))

    y, m = sy, sm
    while (y < ey) or (y == ey and m <= em):
        yield y, m
        m += 1
        if m == 13:
            m = 1
            y += 1


def build_b3_di1_range_node(
    raw_b3_price_report_zip_paths: list[str],
    trusted_ref_calendar_bd_index: pd.DataFrame,
    trusted_b3_di1_instrument_master: pd.DataFrame,
    b3_di1_range: dict,
    b3_price_report: dict,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Outputs:
      - quotes_by_month: {"YYYY-MM": df, ...}  -> PartitionedDataset
      - lineage_by_month: {"YYYY-MM": df, ...} -> PartitionedDataset
      - instrument_master_updated_df: df (GLOBAL)
    """
    start_month = b3_di1_range["start_month"]
    end_month = b3_di1_range["end_month"]
    head_bytes = int(b3_price_report.get("head_bytes", 64000))

    quotes_by_month: Dict[str, pd.DataFrame] = {}
    lineage_by_month: Dict[str, pd.DataFrame] = {}

    # vai sendo atualizado mês a mês
    instr_df = trusted_b3_di1_instrument_master

    for year, month in _iter_months(start_month, end_month):
        key = f"{year:04d}-{month:02d}"

        quotes_df, lineage_df, instr_df = build_b3_di1_trusted_month(
            raw_zip_paths=raw_b3_price_report_zip_paths,
            bd_index_df=trusted_ref_calendar_bd_index,
            year=year,
            month=month,
            head_bytes=head_bytes,
            previous_instrument_master_df=instr_df,  # acumulando global
        )

        quotes_by_month[key] = quotes_df
        lineage_by_month[key] = lineage_df

    return quotes_by_month, lineage_by_month, instr_df
