from __future__ import annotations

from tqdm.auto import tqdm

from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from engine_product.calendars.business_calendar import BusinessCalendar
from engine_product.calendars.repository import DataFrameCalendarRepository
from engine_product.convention.conventions import BU252

from engine_product.instruments.public_bonds import (
    LTNContract,
    NTNFContract,
)

from engine_product.pricing import YieldProblem, yield_to_maturity

from engine_product.risk import (
    macaulay_duration,
    modified_duration,
)


def execute_duckdb_sql_files(
    duckdb_path: str,
    sql_files: list[str],
) -> None:
    """
    Execute SQL files in DuckDB.

    This node creates or replaces the SQL views used before the Python
    curve-input mart calculation.
    """
    with duckdb.connect(duckdb_path) as con:
        for sql_file in sql_files:
            sql_path = Path(sql_file)
            sql = sql_path.read_text(encoding="utf-8")
            con.execute(sql)


def load_public_bond_curve_candidates_from_duckdb(
    duckdb_path: str,
) -> pd.DataFrame:
    """
    Load eligible public bond observations from DuckDB.
    """
    with duckdb.connect(duckdb_path, read_only=True) as con:
        return con.sql(
            """
            SELECT *
            FROM mart_public_bonds_curve_candidates
            """
        ).df()


def load_refined_calendar_from_duckdb(
    duckdb_path: str,
) -> pd.DataFrame:
    """
    Load refined Brazilian calendar from DuckDB.
    """
    with duckdb.connect(duckdb_path, read_only=True) as con:
        return con.sql(
            """
            SELECT *
            FROM vw_refined_calendar_br
            """
        ).df()


def as_date(value: Any) -> date:
    """
    Convert DuckDB/Pandas date-like values to Python datetime.date.

    The product engine currently works correctly when all dates are passed
    as datetime.date, matching the notebook usage.
    """
    if isinstance(value, pd.Timestamp):
        return value.date()

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    return pd.to_datetime(value).date()


def get_row_value(row: Any, *names: str) -> Any:
    """
    Read the first available attribute from a row.

    This allows the node to work whether the SQL view exposes:
        - maturity_date or maturity
        - issue_date or emissao
    """
    for name in names:
        if hasattr(row, name):
            return getattr(row, name)

    raise AttributeError(f"None of these columns were found in row: {names}")


def normalize_rate(value: Any) -> float | None:
    """
    Convert rate to decimal format.

    Examples
    --------
    13.25  -> 0.1325
    0.1325 -> 0.1325
    """
    if value is None or pd.isna(value):
        return None

    value = float(value)

    if abs(value) > 1.0:
        return value / 100.0

    return value


def solver_method_value(method: Any) -> str:
    """
    Return solver method as a clean string.
    """
    if hasattr(method, "value"):
        return method.value

    return str(method)


def build_business_calendar(
    calendar_df: pd.DataFrame,
) -> tuple[BusinessCalendar, BU252]:
    """
    Build BusinessCalendar and BU252 day count from refined calendar data.

    Important:
    This intentionally mirrors the working notebook logic:

        calendar_df["date"] = pd.to_datetime(calendar_df["date"]).dt.date

    This keeps the product engine operating with datetime.date.
    """
    calendar_df = calendar_df.copy()

    calendar_df["date"] = pd.to_datetime(calendar_df["date"]).dt.date
    calendar_df["is_business_day"] = calendar_df["is_business_day"].astype(bool)

    calendar_repo = DataFrameCalendarRepository(calendar_df)
    calendar = BusinessCalendar(calendar_repo)
    bu252 = BU252(calendar)

    return calendar, bu252


def price_cashflows_from_yield(
    cashflows: list,
    ytm: float,
    settlement_date: date,
    day_count: BU252,
) -> float:
    """
    Price future cashflows using an effective annual yield.

    This is used when LTN has observed TAXA_MED but missing PU_MED.
    """
    settlement_date = as_date(settlement_date)
    price = 0.0

    for cf in cashflows:
        payment_date = as_date(cf.payment_date)

        if payment_date <= settlement_date:
            continue

        t = day_count.year_fraction(settlement_date, payment_date)
        price += cf.amount / ((1.0 + ytm) ** t)

    return price


def compute_ltn_curve_input_row(
    row: Any,
    calendar: BusinessCalendar,
    day_count: BU252,
) -> dict:
    """
    Compute curve input measures for one LTN observation.

    Rules
    -----
    LTN + PU_MED:
        market_pu = pu_med
        market_ytm = yield implied from observed PU

    LTN + TAXA_MED:
        market_ytm = taxa_med
        market_pu = price implied from observed yield
    """
    ref_date = as_date(get_row_value(row, "ref_date"))
    issue_date = as_date(get_row_value(row, "issue_date", "emissao"))
    maturity_date = as_date(get_row_value(row, "maturity_date", "maturity"))

    ltn = LTNContract(
        start_date=issue_date,
        maturity_date=maturity_date,
        calendar=calendar,
    )

    cashflows = ltn.build_cashflows(as_of_date=ref_date)

    if row.primary_quote_type == "PRICE":
        market_pu = float(row.pu_med)

        problem = YieldProblem(
            cashflows=cashflows,
            market_price=market_pu,
            settlement_date=ref_date,
            day_count=day_count,
        )

        result = yield_to_maturity(problem)

        market_ytm = result.ytm
        market_pu_source = "PU_MED"
        market_ytm_source = "IMPLIED_FROM_PU_MED"
        solver_method = solver_method_value(result.method)
        solver_iterations = result.iterations

    elif row.primary_quote_type == "YIELD":
        market_ytm = normalize_rate(row.taxa_med)

        if market_ytm is None:
            raise ValueError(
                f"LTN {row.isin} at {ref_date} marked as YIELD, "
                "but taxa_med is missing."
            )

        market_pu = price_cashflows_from_yield(
            cashflows=cashflows,
            ytm=market_ytm,
            settlement_date=ref_date,
            day_count=day_count,
        )

        market_pu_source = "IMPLIED_FROM_TAXA_MED"
        market_ytm_source = "TAXA_MED"
        solver_method = "OBSERVED_YIELD"
        solver_iterations = None

    else:
        raise ValueError(
            f"Unsupported primary_quote_type for LTN {row.isin}: "
            f"{row.primary_quote_type}"
        )

    mac = macaulay_duration(
        cashflows=cashflows,
        ytm=market_ytm,
        settlement_date=ref_date,
        day_count=day_count,
    )

    mod = modified_duration(
        cashflows=cashflows,
        ytm=market_ytm,
        settlement_date=ref_date,
        day_count=day_count,
    )

    return {
        "ref_date": ref_date,
        "instrument_type": row.instrument_type,
        "isin": row.isin,
        "issue_date": issue_date,
        "maturity_date": maturity_date,
        "bd_to_maturity": int(row.bd_to_maturity),
        "market_pu": market_pu,
        "market_ytm": market_ytm,
        "macaulay_duration": mac,
        "modified_duration": mod,
        "quote_quality": row.quote_quality,
        "quote_source": row.quote_source,
        "primary_quote_type": row.primary_quote_type,
        "market_pu_source": market_pu_source,
        "market_ytm_source": market_ytm_source,
        "solver_method": solver_method,
        "solver_iterations": solver_iterations,
    }


def compute_ntnf_curve_input_row(
    row: Any,
    calendar: BusinessCalendar,
    day_count: BU252,
) -> dict:
    """
    Compute curve input measures for one NTN-F observation.

    Rule
    ----
    NTN-F requires observed PU_MED.

    TAXA_MED is not treated as market yield for NTN-F in this project,
    because the recurring 10% value is the contractual coupon rate.
    """
    ref_date = as_date(get_row_value(row, "ref_date"))
    issue_date = as_date(get_row_value(row, "issue_date", "emissao"))
    maturity_date = as_date(get_row_value(row, "maturity_date", "maturity"))

    if row.primary_quote_type != "PRICE":
        raise ValueError(
            f"NTN-F {row.isin} at {ref_date} requires observed PU. "
            f"Got primary_quote_type={row.primary_quote_type}."
        )

    market_pu = float(row.pu_med)

    ntnf = NTNFContract(
        start_date=issue_date,
        maturity_date=maturity_date,
        calendar=calendar,
        day_count=day_count,
    )

    cashflows = ntnf.build_cashflows(as_of_date=ref_date)

    problem = YieldProblem(
        cashflows=cashflows,
        market_price=market_pu,
        settlement_date=ref_date,
        day_count=day_count,
    )

    result = yield_to_maturity(problem)
    market_ytm = result.ytm

    mac = macaulay_duration(
        cashflows=cashflows,
        ytm=market_ytm,
        settlement_date=ref_date,
        day_count=day_count,
    )

    mod = modified_duration(
        cashflows=cashflows,
        ytm=market_ytm,
        settlement_date=ref_date,
        day_count=day_count,
    )

    return {
        "ref_date": ref_date,
        "instrument_type": row.instrument_type,
        "isin": row.isin,
        "issue_date": issue_date,
        "maturity_date": maturity_date,
        "bd_to_maturity": int(row.bd_to_maturity),
        "market_pu": market_pu,
        "market_ytm": market_ytm,
        "macaulay_duration": mac,
        "modified_duration": mod,
        "quote_quality": row.quote_quality,
        "quote_source": row.quote_source,
        "primary_quote_type": row.primary_quote_type,
        "market_pu_source": "PU_MED",
        "market_ytm_source": "IMPLIED_FROM_PU_MED",
        "solver_method": solver_method_value(result.method),
        "solver_iterations": result.iterations,
    }


def build_public_bonds_curve_inputs(
    curve_candidates: pd.DataFrame,
    calendar_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build the final analytical mart consumed by the curve factory.

    Successful calculations go to:
        mart_public_bonds_curve_inputs

    Failed calculations go to:
        mart_public_bonds_curve_calculation_failures

    This prevents one bad quote/instrument from cancelling the whole batch.
    """
    calendar, bu252 = build_business_calendar(calendar_df)

    rows: list[dict] = []
    failures: list[dict] = []

    iterator = tqdm(
                curve_candidates.itertuples(index=False),
                total=len(curve_candidates),
                desc="Building public bonds curve inputs",
                unit="bond",
                )

    for row in iterator:
        try:
            if row.instrument_type == "LTN":
                result_row = compute_ltn_curve_input_row(
                    row=row,
                    calendar=calendar,
                    day_count=bu252,
                )

            elif row.instrument_type == "NTN-F":
                result_row = compute_ntnf_curve_input_row(
                    row=row,
                    calendar=calendar,
                    day_count=bu252,
                )

            else:
                raise ValueError(
                    f"Unsupported instrument_type reached curve input node: "
                    f"{row.instrument_type}"
                )

            rows.append(result_row)

        except Exception as exc:
            failures.append(
                {
                    "ref_date": as_date(get_row_value(row, "ref_date")),
                    "instrument_type": getattr(row, "instrument_type", None),
                    "isin": getattr(row, "isin", None),
                    "issue_date": as_date(get_row_value(row, "issue_date", "emissao")),
                    "maturity_date": as_date(get_row_value(row, "maturity_date", "maturity")),
                    "bd_to_maturity": getattr(row, "bd_to_maturity", None),
                    "pu_med": getattr(row, "pu_med", None),
                    "taxa_med": getattr(row, "taxa_med", None),
                    "quote_quality": getattr(row, "quote_quality", None),
                    "quote_source": getattr(row, "quote_source", None),
                    "primary_quote_type": getattr(row, "primary_quote_type", None),
                    "calculation_status": "FAILED",
                    "calculation_error_type": type(exc).__name__,
                    "calculation_error_message": str(exc),
                }
            )

    input_columns = [
        "ref_date",
        "instrument_type",
        "isin",
        "issue_date",
        "maturity_date",
        "bd_to_maturity",
        "market_pu",
        "market_ytm",
        "macaulay_duration",
        "modified_duration",
        "quote_quality",
        "quote_source",
        "primary_quote_type",
        "market_pu_source",
        "market_ytm_source",
        "solver_method",
        "solver_iterations",
    ]

    failure_columns = [
        "ref_date",
        "instrument_type",
        "isin",
        "issue_date",
        "maturity_date",
        "bd_to_maturity",
        "pu_med",
        "taxa_med",
        "quote_quality",
        "quote_source",
        "primary_quote_type",
        "calculation_status",
        "calculation_error_type",
        "calculation_error_message",
    ]

    if rows:
        inputs = pd.DataFrame(rows)
        inputs = inputs[input_columns]
        inputs = (
            inputs.sort_values(
                ["ref_date", "instrument_type", "maturity_date", "isin"]
            )
            .reset_index(drop=True)
        )
    else:
        inputs = pd.DataFrame(columns=input_columns)

    if failures:
        calculation_failures = pd.DataFrame(failures)
        calculation_failures = calculation_failures[failure_columns]
        calculation_failures = (
            calculation_failures.sort_values(
                ["ref_date", "instrument_type", "maturity_date", "isin"]
            )
            .reset_index(drop=True)
        )
    else:
        calculation_failures = pd.DataFrame(columns=failure_columns)

    return inputs, calculation_failures