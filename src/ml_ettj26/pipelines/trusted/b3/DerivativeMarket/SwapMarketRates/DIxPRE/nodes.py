from __future__ import annotations

from dataclasses import asdict
import pandas as pd

from ml_ettj26.domain.b3_DerivativeMarket.SwapMarketRates.mapper import SwapLineMapper, SwapLineMapperConfig
from ml_ettj26.domain.b3_DerivativeMarket.SwapMarketRates.product_rules import SwapProductSpec
from ml_ettj26.domain.b3_DerivativeMarket.SwapMarketRates.service import SwapTrustedBuilderService
from ml_ettj26.domain.b3_DerivativeMarket.SwapMarketRates.zip_reader import NestedZipReader
from ml_ettj26.domain.b3_DerivativeMarket.SwapMarketRates.file_discovery import list_zip_files_in_date_range


def dataclasses_to_dataframe(items: list[object]) -> pd.DataFrame:
    if not items:
        return pd.DataFrame()
    return pd.DataFrame([asdict(item) for item in items])


def build_b3_swap_trusted_range_partitioned(
    raw_dir: str,
    start_date: str,
    end_date: str,
    product_specs_by_code: dict[str, dict] | None = None,
    product_specs_by_name: dict[str, dict] | None = None,
    encoding: str = "cp1252",
    target_cod_prod: str | None = None,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, pd.DataFrame]:
    specs_by_code = None
    if product_specs_by_code:
        specs_by_code = {
            key: SwapProductSpec(**value)
            for key, value in product_specs_by_code.items()
        }

    specs_by_name = None
    if product_specs_by_name:
        specs_by_name = {
            key: SwapProductSpec(**value)
            for key, value in product_specs_by_name.items()
        }

    mapper = SwapLineMapper(
        config=SwapLineMapperConfig(adjusted_value_scale=100_000),
        specs_by_code=specs_by_code,
        specs_by_name=specs_by_name,
    )
    service = SwapTrustedBuilderService(mapper=mapper)

    zip_files = list_zip_files_in_date_range(
        raw_dir=raw_dir,
        start_date=start_date,
        end_date=end_date,
    )

    swap_partitions: dict[str, pd.DataFrame] = {}
    master_frames: list[pd.DataFrame] = []
    lineage_frames: list[pd.DataFrame] = []

    for zip_path in zip_files:
        reader = NestedZipReader(outer_zip_path=zip_path)
        payload = reader.read_embedded_txt(encoding=encoding)

        result = service.build_from_payload(
            outer_zip=payload.outer_zip,
            inner_zip=payload.inner_zip,
            txt_name=payload.txt_name,
            text=payload.text,
            hash_file=payload.hash_file,
        )

        df_swap = dataclasses_to_dataframe(result.swap_dixpre)
        df_master = dataclasses_to_dataframe(result.swap_master)
        df_lineage = dataclasses_to_dataframe(result.data_lineage)

        if target_cod_prod:
            if not df_swap.empty:
                df_swap = df_swap[df_swap["CodProd"] == target_cod_prod].copy()
            if not df_master.empty:
                df_master = df_master[df_master["CodProd"] == target_cod_prod].copy()

        if not df_swap.empty:
            trade_dates = sorted(
                pd.to_datetime(df_swap["TradDt"]).dt.strftime("%Y-%m-%d").unique()
            )

            for trade_date in trade_dates:
                df_swap_dt = df_swap[
                    pd.to_datetime(df_swap["TradDt"]).dt.strftime("%Y-%m-%d") == trade_date
                ].copy()

                if not df_swap_dt.empty:
                    df_swap_dt = df_swap_dt.drop_duplicates(
                        subset=["TradDt", "CodProd", "days_to_maturity"],
                        keep="last",
                    )
                    swap_partitions[trade_date] = df_swap_dt

        if not df_master.empty:
            master_frames.append(df_master)

        if not df_lineage.empty:
            lineage_frames.append(df_lineage)

    df_swap_master = (
        pd.concat(master_frames, ignore_index=True)
        if master_frames else pd.DataFrame()
    )
    if not df_swap_master.empty:
        df_swap_master = df_swap_master.drop_duplicates(
            subset=["CodProd"],
            keep="last",
        )

    df_data_lineage = (
        pd.concat(lineage_frames, ignore_index=True)
        if lineage_frames else pd.DataFrame()
    )
    if not df_data_lineage.empty:
        df_data_lineage = df_data_lineage.drop_duplicates(
            subset=["lineage_id"],
            keep="last",
        )

    return swap_partitions, df_swap_master, df_data_lineage
