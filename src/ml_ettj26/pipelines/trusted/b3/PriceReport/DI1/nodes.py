from __future__ import annotations

from typing import Dict, Tuple
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
      - quotes_daily: {"YYYY-MM-DD": df, ...}  -> PartitionedDataset
      - lineage_daily: {"YYYY-MM-DD": df, ...} -> PartitionedDataset
      - instrument_master_updated_df: df (GLOBAL)
    """
    start_month = b3_di1_range["start_month"]
    end_month = b3_di1_range["end_month"]
    head_bytes = int(b3_price_report.get("head_bytes", 64000))

    quotes_daily: Dict[str, pd.DataFrame] = {}
    lineage_daily: Dict[str, pd.DataFrame] = {}

    # vai sendo atualizado mês a mês
    instr_df = trusted_b3_di1_instrument_master

    for year, month in _iter_months(start_month, end_month):
        quotes_df, lineage_df, instr_df = build_b3_di1_trusted_month(
            raw_zip_paths=raw_b3_price_report_zip_paths,
            bd_index_df=trusted_ref_calendar_bd_index,
            year=year,
            month=month,
            head_bytes=head_bytes,
            previous_instrument_master_df=instr_df,  # acumulando global
        )

        if not quotes_df.empty:
            trade_dates = sorted(
                pd.to_datetime(quotes_df["TradDt"], utc=True).dt.strftime("%Y-%m-%d").unique()
            )
            for trade_date in trade_dates:
                quotes_daily[trade_date] = quotes_df[
                    pd.to_datetime(quotes_df["TradDt"], utc=True).dt.strftime("%Y-%m-%d") == trade_date
                ].copy()

        if not lineage_df.empty and not quotes_df.empty:
            lineage_by_id = lineage_df.set_index("lineage_id", drop=False)
            quote_dates = (
                quotes_df[["lineage_id", "TradDt"]]
                .assign(trade_date=pd.to_datetime(quotes_df["TradDt"], utc=True).dt.strftime("%Y-%m-%d"))
                [["lineage_id", "trade_date"]]
                .drop_duplicates()
            )
            for trade_date, ids_df in quote_dates.groupby("trade_date", sort=True):
                lineage_daily[trade_date] = lineage_by_id.loc[ids_df["lineage_id"]].reset_index(drop=True).copy()

    return quotes_daily, lineage_daily, instr_df
