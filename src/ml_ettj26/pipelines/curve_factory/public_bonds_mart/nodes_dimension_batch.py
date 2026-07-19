from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
from tqdm.auto import tqdm

from engine_product.pricing import YieldProblem, yield_to_maturity_batch
from engine_product.pricing.cashflow_arrays import (
    build_bd_index_lookup,
    build_cashflow_schedule_lookup,
    macaulay_duration_from_time_amount_pairs,
    price_from_time_amount_pairs,
)

from .nodes import as_date, get_row_value, normalize_rate, solver_method_value
from .nodes_batch import finalize_curve_inputs, make_failure_row


@dataclass(frozen=True)
class CurveInputArrayProblemContext:
    row: Any
    problem: YieldProblem
    time_amount_pairs: tuple[tuple[float, float], ...]
    ref_date: Any
    issue_date: Any
    maturity_date: Any
    market_pu: float
    market_pu_source: str
    market_ytm_source: str


def make_success_row_from_pairs(
    *,
    row: Any,
    time_amount_pairs: tuple[tuple[float, float], ...],
    ref_date: Any,
    issue_date: Any,
    maturity_date: Any,
    market_pu: float,
    market_ytm: float,
    market_pu_source: str,
    market_ytm_source: str,
    solver_method: str,
    solver_iterations: int | None,
) -> dict:
    mac = macaulay_duration_from_time_amount_pairs(
        time_amount_pairs=time_amount_pairs,
        ytm=market_ytm,
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


def build_public_bonds_curve_inputs_from_cashflow_dimension(
    curve_candidates: pd.DataFrame,
    cashflow_dimension: pd.DataFrame,
    calendar_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build public-bond curve inputs using precomputed cashflow-dimension arrays.

    This node avoids rebuilding contracts, schedules and Cashflow objects for
    each historical observation. It filters cashflows by ref_date/as_of_date
    through bd_index arrays and then uses the batch yield solver.
    """
    bd_index_by_date = build_bd_index_lookup(calendar_df)
    cashflows_by_isin = build_cashflow_schedule_lookup(cashflow_dimension)

    rows: list[dict] = []
    failures: list[dict] = []
    contexts: list[CurveInputArrayProblemContext] = []

    iterator = tqdm(
        curve_candidates.itertuples(index=False),
        total=len(curve_candidates),
        desc="Building public bonds curve inputs from cashflow dimension",
        unit="bond",
    )

    for row in iterator:
        if row.pu_med > 5000.0:
            failures.append(
                make_failure_row(
                    row=row,
                    error_type="InvalidPrice",
                    error_message=f"Market price {row.pu_med} is above Notional (5000.00)",
                )
            )
            continue

        if row.pu_med < 50.0:
            failures.append(
                make_failure_row(
                    row=row,
                    error_type="InvalidPrice",
                    error_message=f"Market price {row.pu_med} is below Notional (90.00)",
                )
            )
            continue


        try:
            ref_date = as_date(get_row_value(row, "ref_date"))
            issue_date = as_date(get_row_value(row, "issue_date", "emissao"))
            maturity_date = as_date(get_row_value(row, "maturity_date", "maturity"))
            ref_bd_index = int(bd_index_by_date[ref_date])
            cashflow_schedule = cashflows_by_isin.get(str(row.isin))

            if cashflow_schedule is None:
                raise ValueError(f"Cashflow dimension not found for isin={row.isin}")

            time_amount_pairs = cashflow_schedule.time_amount_pairs_as_of(
                ref_bd_index=ref_bd_index,
            )

            if not time_amount_pairs:
                raise ValueError(
                    f"No future cashflows found for isin={row.isin} at {ref_date}"
                )

            if row.instrument_type == "LTN" and row.primary_quote_type == "YIELD":
                market_ytm = normalize_rate(row.taxa_med)

                if market_ytm is None:
                    raise ValueError(
                        f"LTN {row.isin} at {ref_date} marked as YIELD, "
                        "but taxa_med is missing."
                    )

                market_pu = price_from_time_amount_pairs(
                    time_amount_pairs=time_amount_pairs,
                    ytm=market_ytm,
                )

                rows.append(
                    make_success_row_from_pairs(
                        row=row,
                        time_amount_pairs=time_amount_pairs,
                        ref_date=ref_date,
                        issue_date=issue_date,
                        maturity_date=maturity_date,
                        market_pu=market_pu,
                        market_ytm=market_ytm,
                        market_pu_source="IMPLIED_FROM_TAXA_MED",
                        market_ytm_source="TAXA_MED",
                        solver_method="OBSERVED_YIELD",
                        solver_iterations=None,
                    )
                )
                continue

            if row.primary_quote_type != "PRICE":
                raise ValueError(
                    f"Unsupported primary_quote_type for {row.instrument_type} "
                    f"{row.isin}: {row.primary_quote_type}"
                )

            market_pu = float(row.pu_med)
            problem = YieldProblem.from_time_amount_pairs(
                time_amount_pairs=time_amount_pairs,
                market_price=market_pu,
            )

            contexts.append(
                CurveInputArrayProblemContext(
                    row=row,
                    problem=problem,
                    time_amount_pairs=time_amount_pairs,
                    ref_date=ref_date,
                    issue_date=issue_date,
                    maturity_date=maturity_date,
                    market_pu=market_pu,
                    market_pu_source="PU_MED",
                    market_ytm_source="IMPLIED_FROM_PU_MED",
                )
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
            make_success_row_from_pairs(
                row=context.row,
                time_amount_pairs=context.time_amount_pairs,
                ref_date=context.ref_date,
                issue_date=context.issue_date,
                maturity_date=context.maturity_date,
                market_pu=context.market_pu,
                market_ytm=result.ytm,
                market_pu_source=context.market_pu_source,
                market_ytm_source=context.market_ytm_source,
                solver_method=solver_method_value(result.method),
                solver_iterations=result.iterations,
            )
        )

    return finalize_curve_inputs(rows, failures)
