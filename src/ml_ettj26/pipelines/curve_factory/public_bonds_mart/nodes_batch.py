from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
from tqdm.auto import tqdm

from engine_product.calendars.business_calendar import BusinessCalendar
from engine_product.cashflows.models import Cashflow
from engine_product.convention.conventions import BU252
from engine_product.instruments.public_bonds import LTNContract, NTNFContract
from engine_product.pricing import YieldProblem, yield_to_maturity_batch
from engine_product.risk import macaulay_duration

from .nodes import (
    as_date,
    build_business_calendar,
    get_row_value,
    normalize_rate,
    price_cashflows_from_yield,
    solver_method_value,
)


@dataclass(frozen=True)
class CurveInputProblemContext:
    row_index: int
    row: Any
    cashflows: list[Cashflow]
    problem: YieldProblem
    ref_date: Any
    issue_date: Any
    maturity_date: Any
    market_pu: float
    market_pu_source: str
    market_ytm_source: str


INPUT_COLUMNS = [
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
    "numero_observacoes_dia",
    "numero_observacoes_curto",
    "numero_observacoes_medio",
    "numero_observacoes_longo",
    "flag_volume",
    "flag_cobertura_tenors",
    "flag_ocupacao_tenors",
    "quote_quality",
    "quote_source",
    "primary_quote_type",
    "market_pu_source",
    "market_ytm_source",
    "solver_method",
    "solver_iterations",
]

FAILURE_COLUMNS = [
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


def make_failure_row(
    row: Any,
    error_type: str,
    error_message: str,
) -> dict:
    return {
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
        "calculation_error_type": error_type,
        "calculation_error_message": error_message,
    }


def make_success_row(
    *,
    row: Any,
    cashflows: list[Cashflow],
    ref_date: Any,
    issue_date: Any,
    maturity_date: Any,
    market_pu: float,
    market_ytm: float,
    market_pu_source: str,
    market_ytm_source: str,
    solver_method: str,
    solver_iterations: int | None,
    day_count: BU252,
) -> dict:
    mac = macaulay_duration(
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
        "modified_duration": mac / (1.0 + market_ytm),
        "numero_observacoes_dia": getattr(row, "numero_observacoes_dia", None),
        "numero_observacoes_curto": getattr(row, "numero_observacoes_curto", None),
        "numero_observacoes_medio": getattr(row, "numero_observacoes_medio", None),
        "numero_observacoes_longo": getattr(row, "numero_observacoes_longo", None),
        "flag_volume": getattr(row, "flag_volume", None),
        "flag_cobertura_tenors": getattr(row, "flag_cobertura_tenors", None),
        "flag_ocupacao_tenors": getattr(row, "flag_ocupacao_tenors", None),
        "quote_quality": row.quote_quality,
        "quote_source": row.quote_source,
        "primary_quote_type": row.primary_quote_type,
        "market_pu_source": market_pu_source,
        "market_ytm_source": market_ytm_source,
        "solver_method": solver_method,
        "solver_iterations": solver_iterations,
    }


def build_ltn_batch_item(
    row_index: int,
    row: Any,
    calendar: BusinessCalendar,
    day_count: BU252,
) -> CurveInputProblemContext | dict:
    ref_date = as_date(get_row_value(row, "ref_date"))
    issue_date = as_date(get_row_value(row, "issue_date", "emissao"))
    maturity_date = as_date(get_row_value(row, "maturity_date", "maturity"))

    ltn = LTNContract(
        start_date=issue_date,
        maturity_date=maturity_date,
        calendar=calendar,
    )
    cashflows = ltn.build_cashflows(as_of_date=ref_date)

    if row.primary_quote_type == "YIELD":
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

        return make_success_row(
            row=row,
            cashflows=cashflows,
            ref_date=ref_date,
            issue_date=issue_date,
            maturity_date=maturity_date,
            market_pu=market_pu,
            market_ytm=market_ytm,
            market_pu_source="IMPLIED_FROM_TAXA_MED",
            market_ytm_source="TAXA_MED",
            solver_method="OBSERVED_YIELD",
            solver_iterations=None,
            day_count=day_count,
        )

    if row.primary_quote_type != "PRICE":
        raise ValueError(
            f"Unsupported primary_quote_type for LTN {row.isin}: "
            f"{row.primary_quote_type}"
        )

    market_pu = float(row.pu_med)
    problem = YieldProblem(
        cashflows=cashflows,
        market_price=market_pu,
        settlement_date=ref_date,
        day_count=day_count,
    )

    return CurveInputProblemContext(
        row_index=row_index,
        row=row,
        cashflows=cashflows,
        problem=problem,
        ref_date=ref_date,
        issue_date=issue_date,
        maturity_date=maturity_date,
        market_pu=market_pu,
        market_pu_source="PU_MED",
        market_ytm_source="IMPLIED_FROM_PU_MED",
    )


def build_ntnf_batch_item(
    row_index: int,
    row: Any,
    calendar: BusinessCalendar,
    day_count: BU252,
) -> CurveInputProblemContext:
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

    return CurveInputProblemContext(
        row_index=row_index,
        row=row,
        cashflows=cashflows,
        problem=problem,
        ref_date=ref_date,
        issue_date=issue_date,
        maturity_date=maturity_date,
        market_pu=market_pu,
        market_pu_source="PU_MED",
        market_ytm_source="IMPLIED_FROM_PU_MED",
    )


def finalize_curve_inputs(
    rows: list[dict],
    failures: list[dict],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if rows:
        inputs = pd.DataFrame(rows)
        inputs = inputs[INPUT_COLUMNS]
        inputs = (
            inputs.sort_values(["ref_date", "instrument_type", "maturity_date", "isin"])
            .reset_index(drop=True)
        )
    else:
        inputs = pd.DataFrame(columns=INPUT_COLUMNS)

    if failures:
        calculation_failures = pd.DataFrame(failures)
        calculation_failures = calculation_failures[FAILURE_COLUMNS]
        calculation_failures = (
            calculation_failures.sort_values(
                ["ref_date", "instrument_type", "maturity_date", "isin"]
            )
            .reset_index(drop=True)
        )
    else:
        calculation_failures = pd.DataFrame(columns=FAILURE_COLUMNS)

    return inputs, calculation_failures


def build_public_bonds_curve_inputs_batch(
    curve_candidates: pd.DataFrame,
    calendar_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build public-bond curve inputs using the batch yield solver.

    PRICE observations are solved together through yield_to_maturity_batch.
    LTN observations quoted directly as YIELD keep the observed yield path.
    Failed problem construction or solver failures are returned per observation.
    """
    calendar, bu252 = build_business_calendar(calendar_df)

    rows: list[dict] = []
    failures: list[dict] = []
    contexts: list[CurveInputProblemContext] = []

    iterator = tqdm(
        curve_candidates.itertuples(index=False),
        total=len(curve_candidates),
        desc="Building public bonds curve inputs batch",
        unit="bond",
    )

    for row_index, row in enumerate(iterator):
        if row.pu_med > 5000.0:
            failures.append(
                make_failure_row(
                    row=row,
                    error_type="InvalidPrice",
                    error_message=f"Market price {row.pu_med} is above Notional (5000.00)",
                )
            )
            continue

        try:
            if row.instrument_type == "LTN":
                item = build_ltn_batch_item(
                    row_index=row_index,
                    row=row,
                    calendar=calendar,
                    day_count=bu252,
                )

                if isinstance(item, dict):
                    rows.append(item)
                else:
                    contexts.append(item)

            elif row.instrument_type == "NTN-F":
                contexts.append(
                    build_ntnf_batch_item(
                        row_index=row_index,
                        row=row,
                        calendar=calendar,
                        day_count=bu252,
                    )
                )

            else:
                raise ValueError(
                    f"Unsupported instrument_type reached curve input node: "
                    f"{row.instrument_type}"
                )

        except Exception as exc:
            failures.append(
                make_failure_row(
                    row=row,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            )

    problems = [context.problem for context in contexts]
    batch_results = yield_to_maturity_batch(problems)

    for batch_result in batch_results:
        context = contexts[batch_result.index]

        if not batch_result.succeeded:
            failures.append(
                make_failure_row(
                    row=context.row,
                    error_type=batch_result.error_type or "YieldSolverError",
                    error_message=batch_result.error_message or "Yield solver failed",
                )
            )
            continue

        result = batch_result.result
        rows.append(
            make_success_row(
                row=context.row,
                cashflows=context.cashflows,
                ref_date=context.ref_date,
                issue_date=context.issue_date,
                maturity_date=context.maturity_date,
                market_pu=context.market_pu,
                market_ytm=result.ytm,
                market_pu_source=context.market_pu_source,
                market_ytm_source=context.market_ytm_source,
                solver_method=solver_method_value(result.method),
                solver_iterations=result.iterations,
                day_count=bu252,
            )
        )

    return finalize_curve_inputs(rows, failures)
