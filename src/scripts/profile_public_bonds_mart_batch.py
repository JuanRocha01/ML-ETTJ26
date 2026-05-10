"""
Profile the public-bonds curve mart batch calculation.

Run from the repository root.

PowerShell:
    python src/scripts/profile_public_bonds_mart_batch.py
    python src/scripts/profile_public_bonds_mart_batch.py --limit 5000
    python src/scripts/profile_public_bonds_mart_batch.py --cprofile-out public_bonds_batch.prof

Bash:
    # python src/scripts/profile_public_bonds_mart_batch.py
    # python src/scripts/profile_public_bonds_mart_batch.py --limit 5000
    # python src/scripts/profile_public_bonds_mart_batch.py --cprofile-out public_bonds_batch.prof
    # python -m pstats public_bonds_batch.prof
    #   sort cumtime
    #   stats 40

If your environment uses uv:
    # uv run python src/scripts/profile_public_bonds_mart_batch.py
    # uv run python src/scripts/profile_public_bonds_mart_batch.py --limit 5000
"""

from __future__ import annotations

import argparse
import cProfile
import json
import pstats
import time
import tracemalloc
from collections import Counter, defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pandas as pd

from engine_product.pricing import yield_to_maturity_batch
from ml_ettj26.pipelines.curve_factory.public_bonds_mart.nodes import (
    build_business_calendar,
    execute_duckdb_sql_files,
    load_public_bond_curve_candidates_from_duckdb,
    load_refined_calendar_from_duckdb,
    solver_method_value,
)
from ml_ettj26.pipelines.curve_factory.public_bonds_mart.nodes_batch import (
    CurveInputProblemContext,
    build_ltn_batch_item,
    build_ntnf_batch_item,
    finalize_curve_inputs,
    make_failure_row,
    make_success_row,
)


DEFAULT_SQL_FILES = [
    "sql/marts/public_bonds/01_mart_public_bonds_quotes_quality.sql",
    "sql/marts/public_bonds/02_mart_public_bonds_curve_candidates_and_exclusions.sql",
]


@contextmanager
def timed_step(name: str, timings: dict[str, float]):
    start = time.perf_counter()
    try:
        yield
    finally:
        timings[name] = time.perf_counter() - start


def dataframe_memory_mb(df: pd.DataFrame) -> float:
    return float(df.memory_usage(deep=True).sum() / 1024 / 1024)


def current_memory_mb() -> dict[str, float]:
    current, peak = tracemalloc.get_traced_memory()
    return {
        "current_mb": current / 1024 / 1024,
        "peak_mb": peak / 1024 / 1024,
    }


def top_tracemalloc_lines(limit: int = 10) -> list[dict[str, Any]]:
    snapshot = tracemalloc.take_snapshot()
    stats = snapshot.statistics("lineno")[:limit]

    return [
        {
            "file": str(stat.traceback[0].filename),
            "line": stat.traceback[0].lineno,
            "size_mb": stat.size / 1024 / 1024,
            "count": stat.count,
        }
        for stat in stats
    ]


def safe_rate(count: int | float, seconds: float) -> float:
    if seconds <= 0.0:
        return 0.0

    return float(count / seconds)


def summarize_batch_results(batch_results) -> dict[str, Any]:
    method_counts = Counter()
    error_counts = Counter()

    for item in batch_results:
        if item.succeeded:
            method_counts[solver_method_value(item.result.method)] += 1
        else:
            error_counts[item.error_type or "UnknownError"] += 1

    return {
        "success_count": sum(method_counts.values()),
        "failure_count": sum(error_counts.values()),
        "method_counts": dict(method_counts),
        "error_counts": dict(error_counts),
    }


def run_diagnostics(args: argparse.Namespace) -> dict[str, Any]:
    timings: dict[str, float] = {}
    counts: dict[str, Any] = {}
    memory_marks: dict[str, Any] = {}

    tracemalloc.start()

    with timed_step("execute_duckdb_sql_files", timings):
        execute_duckdb_sql_files(
            duckdb_path=args.duckdb_path,
            sql_files=args.sql_files,
        )
    memory_marks["after_sql"] = current_memory_mb()

    with timed_step("load_curve_candidates", timings):
        curve_candidates = load_public_bond_curve_candidates_from_duckdb(
            duckdb_path=args.duckdb_path,
        )

    if args.limit is not None:
        curve_candidates = curve_candidates.head(args.limit).copy()

    memory_marks["after_candidates_load"] = current_memory_mb()

    with timed_step("load_calendar", timings):
        calendar_df = load_refined_calendar_from_duckdb(
            duckdb_path=args.duckdb_path,
        )
    memory_marks["after_calendar_load"] = current_memory_mb()

    counts["curve_candidates_rows"] = int(len(curve_candidates))
    counts["curve_candidates_memory_mb"] = dataframe_memory_mb(curve_candidates)
    counts["calendar_rows"] = int(len(calendar_df))
    counts["calendar_memory_mb"] = dataframe_memory_mb(calendar_df)

    with timed_step("build_business_calendar", timings):
        calendar, bu252 = build_business_calendar(calendar_df)
    memory_marks["after_business_calendar"] = current_memory_mb()

    rows: list[dict] = []
    failures: list[dict] = []
    contexts: list[CurveInputProblemContext] = []
    row_prepare_counts = Counter()
    row_prepare_errors = Counter()
    cashflow_count_by_instrument = defaultdict(Counter)

    with timed_step("prepare_rows_cashflows_and_problems", timings):
        for row_index, row in enumerate(curve_candidates.itertuples(index=False)):
            row_prepare_counts[f"{row.instrument_type}:{row.primary_quote_type}"] += 1

            if row.pu_med > 5000.0:
                failures.append(
                    make_failure_row(
                        row=row,
                        error_type="InvalidPrice",
                        error_message=(
                            f"Market price {row.pu_med} is above Notional (5000.00)"
                        ),
                    )
                )
                row_prepare_errors["InvalidPrice"] += 1
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
                        cashflow_count_by_instrument[row.instrument_type][
                            len(item.problem.time_amount_pairs)
                        ] += 1

                elif row.instrument_type == "NTN-F":
                    item = build_ntnf_batch_item(
                        row_index=row_index,
                        row=row,
                        calendar=calendar,
                        day_count=bu252,
                    )
                    contexts.append(item)
                    cashflow_count_by_instrument[row.instrument_type][
                        len(item.problem.time_amount_pairs)
                    ] += 1

                else:
                    raise ValueError(
                        "Unsupported instrument_type reached curve input node: "
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
                row_prepare_errors[type(exc).__name__] += 1

    memory_marks["after_prepare"] = current_memory_mb()

    problems = [context.problem for context in contexts]
    counts["observed_yield_rows_pre_solved"] = int(len(rows))
    counts["problem_count_for_batch_solver"] = int(len(problems))
    counts["prepare_counts"] = dict(row_prepare_counts)
    counts["prepare_error_counts"] = dict(row_prepare_errors)
    counts["cashflow_count_by_instrument"] = {
        instrument: dict(counter)
        for instrument, counter in cashflow_count_by_instrument.items()
    }

    with timed_step("yield_to_maturity_batch", timings):
        batch_results = yield_to_maturity_batch(problems)
    memory_marks["after_solver"] = current_memory_mb()

    counts["solver_summary"] = summarize_batch_results(batch_results)

    with timed_step("finalize_success_rows_and_durations", timings):
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
    memory_marks["after_success_rows"] = current_memory_mb()

    with timed_step("build_and_sort_output_dataframes", timings):
        inputs, calculation_failures = finalize_curve_inputs(rows, failures)
    memory_marks["after_output_dataframes"] = current_memory_mb()

    counts["output_rows"] = int(len(inputs))
    counts["failure_rows"] = int(len(calculation_failures))
    counts["output_memory_mb"] = dataframe_memory_mb(inputs)
    counts["failures_memory_mb"] = dataframe_memory_mb(calculation_failures)

    total_timed_seconds = sum(timings.values())
    timings["total_timed_seconds"] = total_timed_seconds
    curve_candidate_rows = counts["curve_candidates_rows"]
    mart_calculation_seconds = (
        timings["prepare_rows_cashflows_and_problems"]
        + timings["yield_to_maturity_batch"]
        + timings["finalize_success_rows_and_durations"]
    )

    throughput = {
        "curve_candidate_rows": curve_candidate_rows,
        "problem_count_for_batch_solver": counts["problem_count_for_batch_solver"],
        "mart_calculation_seconds": mart_calculation_seconds,
        "mart_calculation_bonds_per_second": safe_rate(
            curve_candidate_rows,
            mart_calculation_seconds,
        ),
        "solver_seconds": timings["yield_to_maturity_batch"],
        "solver_problems_per_second": safe_rate(
            counts["problem_count_for_batch_solver"],
            timings["yield_to_maturity_batch"],
        ),
        "end_to_end_seconds": total_timed_seconds,
        "end_to_end_bonds_per_second": safe_rate(
            curve_candidate_rows,
            total_timed_seconds,
        ),
    }

    return {
        "timings_seconds": timings,
        "timing_share_pct": {
            key: (value / total_timed_seconds * 100.0 if total_timed_seconds else 0.0)
            for key, value in timings.items()
            if key != "total_timed_seconds"
        },
        "throughput": throughput,
        "counts": counts,
        "memory": memory_marks,
        "top_memory_lines": top_tracemalloc_lines(args.top_memory_lines),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile public-bonds curve mart batch processing.",
    )
    parser.add_argument(
        "--duckdb-path",
        default="data/duckdb/ml_ettj26.duckdb",
        help="Path to the DuckDB database.",
    )
    parser.add_argument(
        "--sql-files",
        nargs="+",
        default=DEFAULT_SQL_FILES,
        help="SQL files used to create the mart candidate views.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional row limit for faster diagnostic runs.",
    )
    parser.add_argument(
        "--top-memory-lines",
        type=int,
        default=15,
        help="Number of top tracemalloc source lines to print.",
    )
    parser.add_argument(
        "--cprofile-out",
        default=None,
        help="Optional path to write a cProfile .prof file.",
    )
    parser.add_argument(
        "--pstats-top",
        type=int,
        default=40,
        help="Number of cProfile rows to print when --cprofile-out is used.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.cprofile_out:
        profiler = cProfile.Profile()
        profiler.enable()
        report = run_diagnostics(args)
        profiler.disable()
        profiler.dump_stats(args.cprofile_out)

        stats = pstats.Stats(profiler).sort_stats("cumtime")
        stats.print_stats(args.pstats_top)
    else:
        report = run_diagnostics(args)

    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
