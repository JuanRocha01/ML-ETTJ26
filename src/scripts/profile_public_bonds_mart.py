"""
Profile the current public-bonds curve mart calculation.

Run from the repository root.

PowerShell:
    python src/scripts/profile_public_bonds_mart.py
    python src/scripts/profile_public_bonds_mart.py --limit 5000
    python src/scripts/profile_public_bonds_mart.py --cprofile-out public_bonds_mart.prof

Bash:
    # python src/scripts/profile_public_bonds_mart.py
    # python src/scripts/profile_public_bonds_mart.py --limit 5000
    # python src/scripts/profile_public_bonds_mart.py --cprofile-out public_bonds_mart.prof
    # python -m pstats public_bonds_mart.prof
    #   sort cumtime
    #   stats 40

If your environment uses uv:
    # uv run python src/scripts/profile_public_bonds_mart.py
    # uv run python src/scripts/profile_public_bonds_mart.py --limit 5000
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
from typing import Any

import pandas as pd

from ml_ettj26.pipelines.curve_factory.public_bonds_mart.nodes import (
    as_date,
    build_business_calendar,
    compute_ltn_curve_input_row,
    compute_ntnf_curve_input_row,
    execute_duckdb_sql_files,
    get_row_value,
    load_public_bond_curve_candidates_from_duckdb,
    load_refined_calendar_from_duckdb,
)


DEFAULT_SQL_FILES = [
    "sql/marts/public_bonds/01_mart_public_bonds_quotes_quality.sql",
    "sql/marts/public_bonds/02_mart_public_bonds_curve_candidates_and_exclusions.sql",
]

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
    "flag_volume",
    "flag_cobertura_tenors",
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


def make_failure_row(row: Any, error_type: str, error_message: str) -> dict:
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


def run_diagnostics(args: argparse.Namespace) -> dict[str, Any]:
    timings: dict[str, float] = {}
    counts: dict[str, Any] = {}
    memory_marks: dict[str, Any] = {}

    row_time_by_bucket = defaultdict(float)
    row_counts_by_bucket = Counter()
    row_success_counts = Counter()
    row_error_counts = Counter()
    solver_method_counts = Counter()

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

    with timed_step("row_loop_compute_yields_and_durations", timings):
        for row in curve_candidates.itertuples(index=False):
            bucket = f"{row.instrument_type}:{row.primary_quote_type}"
            row_counts_by_bucket[bucket] += 1

            row_start = time.perf_counter()

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
                row_error_counts["InvalidPrice"] += 1
                row_time_by_bucket[bucket] += time.perf_counter() - row_start
                continue

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
                        "Unsupported instrument_type reached curve input node: "
                        f"{row.instrument_type}"
                    )

                rows.append(result_row)
                row_success_counts[bucket] += 1
                solver_method_counts[result_row["solver_method"]] += 1

            except Exception as exc:
                failures.append(
                    make_failure_row(
                        row=row,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                )
                row_error_counts[type(exc).__name__] += 1

            finally:
                row_time_by_bucket[bucket] += time.perf_counter() - row_start

    memory_marks["after_row_loop"] = current_memory_mb()

    with timed_step("build_and_sort_output_dataframes", timings):
        inputs, calculation_failures = finalize_curve_inputs(rows, failures)
    memory_marks["after_output_dataframes"] = current_memory_mb()

    counts["row_counts_by_bucket"] = dict(row_counts_by_bucket)
    counts["row_success_counts_by_bucket"] = dict(row_success_counts)
    counts["row_error_counts"] = dict(row_error_counts)
    counts["solver_method_counts"] = dict(solver_method_counts)
    counts["row_time_seconds_by_bucket"] = dict(row_time_by_bucket)
    counts["row_avg_ms_by_bucket"] = {
        bucket: (
            row_time_by_bucket[bucket] / row_counts_by_bucket[bucket] * 1000.0
            if row_counts_by_bucket[bucket]
            else 0.0
        )
        for bucket in row_counts_by_bucket
    }
    counts["output_rows"] = int(len(inputs))
    counts["failure_rows"] = int(len(calculation_failures))
    counts["output_memory_mb"] = dataframe_memory_mb(inputs)
    counts["failures_memory_mb"] = dataframe_memory_mb(calculation_failures)

    total_timed_seconds = sum(timings.values())
    timings["total_timed_seconds"] = total_timed_seconds

    return {
        "timings_seconds": timings,
        "timing_share_pct": {
            key: (value / total_timed_seconds * 100.0 if total_timed_seconds else 0.0)
            for key, value in timings.items()
            if key != "total_timed_seconds"
        },
        "counts": counts,
        "memory": memory_marks,
        "top_memory_lines": top_tracemalloc_lines(args.top_memory_lines),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile current public-bonds curve mart processing.",
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
