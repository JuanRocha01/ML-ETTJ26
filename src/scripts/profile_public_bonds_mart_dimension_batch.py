"""
Profile the optimized public-bonds YTM flow:

1. build the static public-bond cashflow dimension;
2. calculate curve inputs from that dimension with batch YTM.

Run from the repository root.

PowerShell:
    python src/scripts/profile_public_bonds_mart_dimension_batch.py
    python src/scripts/profile_public_bonds_mart_dimension_batch.py --limit 5000
    python src/scripts/profile_public_bonds_mart_dimension_batch.py --cprofile-out public_bonds_dimension_batch.prof

Bash:
    # python src/scripts/profile_public_bonds_mart_dimension_batch.py
    # python src/scripts/profile_public_bonds_mart_dimension_batch.py --limit 5000
    # python src/scripts/profile_public_bonds_mart_dimension_batch.py --cprofile-out public_bonds_dimension_batch.prof
    # python -m pstats public_bonds_dimension_batch.prof
    #   sort cumtime
    #   stats 40

If your environment uses uv:
    # uv run python src/scripts/profile_public_bonds_mart_dimension_batch.py
    # uv run python src/scripts/profile_public_bonds_mart_dimension_batch.py --limit 5000
"""

from __future__ import annotations

import argparse
import cProfile
import json
import pstats
import time
import tracemalloc
from collections import Counter
from contextlib import contextmanager
from typing import Any

import pandas as pd

from ml_ettj26.pipelines.curve_factory.public_bonds_cashflows.nodes import (
    build_public_bond_cashflow_dimension,
    execute_duckdb_sql_files as execute_cashflow_sql_files,
    load_public_bond_instruments_from_duckdb,
    load_refined_calendar_from_duckdb as load_calendar_for_cashflows,
)
from ml_ettj26.pipelines.curve_factory.public_bonds_mart.nodes import (
    execute_duckdb_sql_files as execute_mart_sql_files,
    load_public_bond_curve_candidates_from_duckdb,
    load_refined_calendar_from_duckdb as load_calendar_for_mart,
    solver_method_value,
)
from ml_ettj26.pipelines.curve_factory.public_bonds_mart.nodes_dimension_batch import (
    build_public_bonds_curve_inputs_from_cashflow_dimension,
)


DEFAULT_CASHFLOW_SQL_FILES = [
    "sql/03_refined/calendar.sql",
    "sql/03_refined/bcb_demab.sql",
]

DEFAULT_MART_SQL_FILES = [
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


def solver_method_counts(inputs: pd.DataFrame) -> dict[str, int]:
    if inputs.empty or "solver_method" not in inputs.columns:
        return {}

    return {
        str(method): int(count)
        for method, count in inputs["solver_method"].value_counts().items()
    }


def cashflow_dimension_summary(cashflow_dimension: pd.DataFrame) -> dict[str, Any]:
    if cashflow_dimension.empty:
        return {
            "cashflow_rows": 0,
            "isin_count": 0,
            "cashflow_type_counts": {},
            "cashflow_rows_by_instrument": {},
        }

    return {
        "cashflow_rows": int(len(cashflow_dimension)),
        "isin_count": int(cashflow_dimension["isin"].nunique()),
        "cashflow_type_counts": {
            str(key): int(value)
            for key, value in cashflow_dimension["cashflow_type"].value_counts().items()
        }
        if "cashflow_type" in cashflow_dimension.columns
        else {},
        "cashflow_rows_by_instrument": {
            str(key): int(value)
            for key, value in cashflow_dimension["instrument_type"].value_counts().items()
        }
        if "instrument_type" in cashflow_dimension.columns
        else {},
        "max_cashflows_per_isin": int(
            cashflow_dimension.groupby("isin").size().max()
        ),
    }


def run_diagnostics(args: argparse.Namespace) -> dict[str, Any]:
    timings: dict[str, float] = {}
    counts: dict[str, Any] = {}
    memory_marks: dict[str, Any] = {}

    tracemalloc.start()

    with timed_step("cashflow_execute_source_sql_files", timings):
        execute_cashflow_sql_files(
            duckdb_path=args.duckdb_path,
            sql_files=args.cashflow_sql_files,
        )
    memory_marks["after_cashflow_source_sql"] = current_memory_mb()

    with timed_step("cashflow_load_instruments", timings):
        instruments = load_public_bond_instruments_from_duckdb(
            duckdb_path=args.duckdb_path,
        )
    memory_marks["after_instruments_load"] = current_memory_mb()

    with timed_step("cashflow_load_calendar", timings):
        calendar_for_cashflows = load_calendar_for_cashflows(
            duckdb_path=args.duckdb_path,
        )
    memory_marks["after_cashflow_calendar_load"] = current_memory_mb()

    with timed_step("cashflow_build_dimension", timings):
        cashflow_dimension = build_public_bond_cashflow_dimension(
            instruments=instruments,
            calendar_df=calendar_for_cashflows,
        )
    memory_marks["after_cashflow_dimension_build"] = current_memory_mb()

    with timed_step("mart_execute_candidate_sql_files", timings):
        execute_mart_sql_files(
            duckdb_path=args.duckdb_path,
            sql_files=args.mart_sql_files,
        )
    memory_marks["after_mart_sql"] = current_memory_mb()

    with timed_step("mart_load_curve_candidates", timings):
        curve_candidates = load_public_bond_curve_candidates_from_duckdb(
            duckdb_path=args.duckdb_path,
        )

    if args.limit is not None:
        curve_candidates = curve_candidates.head(args.limit).copy()

    memory_marks["after_candidates_load"] = current_memory_mb()

    with timed_step("mart_load_calendar", timings):
        calendar_for_mart = load_calendar_for_mart(
            duckdb_path=args.duckdb_path,
        )
    memory_marks["after_mart_calendar_load"] = current_memory_mb()

    with timed_step("mart_dimension_batch_calculation", timings):
        inputs, calculation_failures = (
            build_public_bonds_curve_inputs_from_cashflow_dimension(
                curve_candidates=curve_candidates,
                cashflow_dimension=cashflow_dimension,
                calendar_df=calendar_for_mart,
            )
        )
    memory_marks["after_dimension_batch_calculation"] = current_memory_mb()

    counts["instruments_rows"] = int(len(instruments))
    counts["instrument_type_counts"] = {
        str(key): int(value)
        for key, value in Counter(instruments["instrument_type"]).items()
    }
    counts["instruments_memory_mb"] = dataframe_memory_mb(instruments)

    counts["cashflow_calendar_rows"] = int(len(calendar_for_cashflows))
    counts["cashflow_calendar_memory_mb"] = dataframe_memory_mb(calendar_for_cashflows)
    counts["cashflow_dimension_memory_mb"] = dataframe_memory_mb(cashflow_dimension)
    counts["cashflow_dimension_summary"] = cashflow_dimension_summary(cashflow_dimension)

    counts["curve_candidates_rows"] = int(len(curve_candidates))
    counts["curve_candidates_memory_mb"] = dataframe_memory_mb(curve_candidates)
    counts["mart_calendar_rows"] = int(len(calendar_for_mart))
    counts["mart_calendar_memory_mb"] = dataframe_memory_mb(calendar_for_mart)

    counts["output_rows"] = int(len(inputs))
    counts["failure_rows"] = int(len(calculation_failures))
    counts["output_memory_mb"] = dataframe_memory_mb(inputs)
    counts["failures_memory_mb"] = dataframe_memory_mb(calculation_failures)
    counts["solver_method_counts"] = solver_method_counts(inputs)

    if not calculation_failures.empty:
        counts["failure_error_counts"] = {
            str(key): int(value)
            for key, value in calculation_failures[
                "calculation_error_type"
            ].value_counts().items()
        }
    else:
        counts["failure_error_counts"] = {}

    total_timed_seconds = sum(timings.values())
    timings["total_timed_seconds"] = total_timed_seconds
    curve_candidate_rows = counts["curve_candidates_rows"]
    cashflow_preprocessing_seconds = (
        timings["cashflow_execute_source_sql_files"]
        + timings["cashflow_load_instruments"]
        + timings["cashflow_load_calendar"]
        + timings["cashflow_build_dimension"]
    )
    mart_calculation_seconds = timings["mart_dimension_batch_calculation"]

    throughput = {
        "curve_candidate_rows": curve_candidate_rows,
        "instrument_rows": counts["instruments_rows"],
        "cashflow_rows": counts["cashflow_dimension_summary"]["cashflow_rows"],
        "cashflow_preprocessing_seconds": cashflow_preprocessing_seconds,
        "cashflow_preprocessing_instruments_per_second": safe_rate(
            counts["instruments_rows"],
            cashflow_preprocessing_seconds,
        ),
        "cashflow_preprocessing_cashflows_per_second": safe_rate(
            counts["cashflow_dimension_summary"]["cashflow_rows"],
            cashflow_preprocessing_seconds,
        ),
        "mart_calculation_seconds": mart_calculation_seconds,
        "mart_calculation_bonds_per_second": safe_rate(
            curve_candidate_rows,
            mart_calculation_seconds,
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
        description=(
            "Profile cashflow-dimension preprocessing plus dimension-based "
            "public-bonds curve mart calculation."
        ),
    )
    parser.add_argument(
        "--duckdb-path",
        default="data/duckdb/ml_ettj26.duckdb",
        help="Path to the DuckDB database.",
    )
    parser.add_argument(
        "--cashflow-sql-files",
        nargs="+",
        default=DEFAULT_CASHFLOW_SQL_FILES,
        help="SQL files needed to load instrument/calendar source views.",
    )
    parser.add_argument(
        "--mart-sql-files",
        nargs="+",
        default=DEFAULT_MART_SQL_FILES,
        help="SQL files used to create the mart candidate views.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional curve-candidate row limit for faster diagnostic runs.",
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
