from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from engine_product.calendars.business_calendar import BusinessCalendar
from engine_product.calendars.repository import DataFrameCalendarRepository
from engine_product.cashflows.models import Cashflow, CashflowType
from engine_product.convention.conventions import BU252
from engine_product.instruments.public_bonds import LTNContract, NTNFContract


CASHFLOW_TYPE_RANK = {
    CashflowType.INTEREST: 10,
    CashflowType.AMORTIZATION: 20,
    CashflowType.PRINCIPAL: 30,
    CashflowType.FEE: 40,
}

CASHFLOW_DIMENSION_COLUMNS = [
    "isin",
    "instrument_type",
    "issue_date",
    "maturity_date",
    "cashflow_number",
    "payment_date",
    "payment_bd_index",
    "issue_bd_index",
    "bd_from_issue",
    "cashflow_type",
    "cashflow_type_rank",
    "amount",
    "accrual_start",
    "accrual_end",
    "notional_before",
    "notional_after",
    "metadata_json",
]


def as_date(value: Any) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    return pd.to_datetime(value).date()


def execute_duckdb_sql_files(
    duckdb_path: str,
    sql_files: list[str],
) -> None:
    with duckdb.connect(duckdb_path) as con:
        for sql_file in sql_files:
            sql = Path(sql_file).read_text(encoding="utf-8")
            con.execute(sql)


def load_public_bond_instruments_from_duckdb(
    duckdb_path: str,
) -> pd.DataFrame:
    """
    Load the distinct public-bond instruments that the pricing engine supports.
    """
    with duckdb.connect(duckdb_path, read_only=True) as con:
        return con.sql(
            """
            SELECT DISTINCT
                isin,
                sigla AS instrument_type,
                issue_date,
                maturity AS maturity_date
            FROM vw_refined_bcb_demab_government_bonds_secondary_market
            WHERE sigla IN ('LTN', 'NTN-F')
              AND isin IS NOT NULL
              AND issue_date IS NOT NULL
              AND maturity IS NOT NULL
            ORDER BY isin, issue_date, maturity_date
            """
        ).df()


def load_refined_calendar_from_duckdb(
    duckdb_path: str,
) -> pd.DataFrame:
    with duckdb.connect(duckdb_path, read_only=True) as con:
        return con.sql(
            """
            SELECT *
            FROM vw_refined_calendar_br
            """
        ).df()


def build_business_calendar(
    calendar_df: pd.DataFrame,
) -> tuple[BusinessCalendar, BU252, dict[date, int]]:
    calendar_df = calendar_df.copy()
    calendar_df["date"] = pd.to_datetime(calendar_df["date"]).dt.date
    calendar_df["is_business_day"] = calendar_df["is_business_day"].astype(bool)

    bd_index_by_date = dict(
        zip(calendar_df["date"], calendar_df["bd_index"], strict=True)
    )

    calendar_repo = DataFrameCalendarRepository(calendar_df)
    calendar = BusinessCalendar(calendar_repo)
    day_count = BU252(calendar)

    return calendar, day_count, bd_index_by_date


def cashflow_type_rank(cashflow_type: CashflowType) -> int:
    return CASHFLOW_TYPE_RANK.get(cashflow_type, 90)


def metadata_to_json(metadata: dict | None) -> str | None:
    if metadata is None:
        return None

    return json.dumps(metadata, sort_keys=True, default=str)


def build_contract_cashflows(
    *,
    instrument_type: str,
    issue_date: date,
    maturity_date: date,
    calendar: BusinessCalendar,
    day_count: BU252,
) -> list[Cashflow]:
    if instrument_type == "LTN":
        contract = LTNContract(
            start_date=issue_date,
            maturity_date=maturity_date,
            calendar=calendar,
        )
    elif instrument_type == "NTN-F":
        contract = NTNFContract(
            start_date=issue_date,
            maturity_date=maturity_date,
            calendar=calendar,
            day_count=day_count,
        )
    else:
        raise ValueError(f"Unsupported public bond instrument_type: {instrument_type}")

    return contract.build_cashflows(as_of_date=issue_date)


def cashflow_to_row(
    *,
    isin: str,
    instrument_type: str,
    issue_date: date,
    maturity_date: date,
    cashflow_number: int,
    cashflow: Cashflow,
    bd_index_by_date: dict[date, int],
) -> dict:
    payment_date = as_date(cashflow.payment_date)
    issue_bd_index = int(bd_index_by_date[issue_date])
    payment_bd_index = int(bd_index_by_date[payment_date])

    return {
        "isin": isin,
        "instrument_type": instrument_type,
        "issue_date": issue_date,
        "maturity_date": maturity_date,
        "cashflow_number": cashflow_number,
        "payment_date": payment_date,
        "payment_bd_index": payment_bd_index,
        "issue_bd_index": issue_bd_index,
        "bd_from_issue": payment_bd_index - issue_bd_index,
        "cashflow_type": cashflow.cashflow_type.value,
        "cashflow_type_rank": cashflow_type_rank(cashflow.cashflow_type),
        "amount": cashflow.amount,
        "accrual_start": cashflow.accrual_start,
        "accrual_end": cashflow.accrual_end,
        "notional_before": cashflow.notional_before,
        "notional_after": cashflow.notional_after,
        "metadata_json": metadata_to_json(cashflow.metadata),
    }


def build_public_bond_cashflow_dimension(
    instruments: pd.DataFrame,
    calendar_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build a static cashflow dimension from the product engine.

    The output keeps one row per Cashflow object, keyed by (isin, cashflow_number).
    Consumers that only need yield pricing can aggregate by payment_bd_index later.
    """
    calendar, day_count, bd_index_by_date = build_business_calendar(calendar_df)

    rows: list[dict] = []

    for instrument in instruments.itertuples(index=False):
        isin = str(instrument.isin)
        instrument_type = str(instrument.instrument_type)
        issue_date = as_date(instrument.issue_date)
        maturity_date = as_date(instrument.maturity_date)

        cashflows = build_contract_cashflows(
            instrument_type=instrument_type,
            issue_date=issue_date,
            maturity_date=maturity_date,
            calendar=calendar,
            day_count=day_count,
        )

        ordered_cashflows = sorted(
            cashflows,
            key=lambda cf: (
                as_date(cf.payment_date),
                cashflow_type_rank(cf.cashflow_type),
            ),
        )

        for cashflow_number, cashflow in enumerate(ordered_cashflows, start=1):
            rows.append(
                cashflow_to_row(
                    isin=isin,
                    instrument_type=instrument_type,
                    issue_date=issue_date,
                    maturity_date=maturity_date,
                    cashflow_number=cashflow_number,
                    cashflow=cashflow,
                    bd_index_by_date=bd_index_by_date,
                )
            )

    if not rows:
        return pd.DataFrame(columns=CASHFLOW_DIMENSION_COLUMNS)

    dimension = pd.DataFrame(rows)
    dimension = dimension[CASHFLOW_DIMENSION_COLUMNS]
    dimension = (
        dimension.sort_values(["isin", "cashflow_number"])
        .reset_index(drop=True)
    )

    duplicated_keys = dimension.duplicated(["isin", "cashflow_number"])

    if duplicated_keys.any():
        duplicates = dimension.loc[duplicated_keys, ["isin", "cashflow_number"]]
        raise ValueError(
            "Duplicate cashflow dimension keys found: "
            f"{duplicates.to_dict(orient='records')}"
        )

    return dimension


def register_public_bond_cashflow_dimension_view(
    duckdb_path: str,
    parquet_path: str,
    cashflow_dimension: pd.DataFrame,
) -> None:
    """
    Register the cashflow dimension parquet as a DuckDB consumption view.

    cashflow_dimension is intentionally accepted to make Kedro run this node
    after the parquet dataset has been materialized.
    """
    normalized_path = Path(parquet_path).resolve().as_posix()

    with duckdb.connect(duckdb_path) as con:
        con.execute(
            f"""
            CREATE OR REPLACE VIEW mart_public_bonds_cashflow_dimension AS
            SELECT *
            FROM read_parquet('{normalized_path}');
            """
        )
