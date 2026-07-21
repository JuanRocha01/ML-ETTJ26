from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

CurvePartition = pd.DataFrame | Callable[[], pd.DataFrame]

_KEY_COLUMNS = ("ref_date", "tenor_bd")
_VIEW_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_and_pivot_curve(
    curve: pd.DataFrame,
    *,
    rate_column: str,
    source_name: str,
) -> pd.DataFrame:
    """Convert one long curve frame into a date x business-day matrix."""

    required = {*_KEY_COLUMNS, rate_column}
    missing = sorted(required.difference(curve.columns))
    if missing:
        raise ValueError(
            f"{source_name} is missing required columns: {missing}"
        )
    if curve.empty:
        raise ValueError(f"{source_name} contains no curve observations")

    selected = curve.loc[:, [*_KEY_COLUMNS, rate_column]].copy()
    selected["ref_date"] = pd.to_datetime(
        selected["ref_date"], errors="raise"
    ).dt.normalize()

    tenor_numeric = pd.to_numeric(selected["tenor_bd"], errors="raise")
    if not np.isfinite(tenor_numeric).all():
        raise ValueError(f"{source_name}.tenor_bd must be finite")
    if not np.equal(tenor_numeric, np.floor(tenor_numeric)).all():
        raise ValueError(f"{source_name}.tenor_bd must contain integers")
    selected["tenor_bd"] = tenor_numeric.astype(np.int64)
    if selected["tenor_bd"].le(0).any():
        raise ValueError(f"{source_name}.tenor_bd must be strictly positive")

    duplicate = selected.duplicated([*_KEY_COLUMNS], keep=False)
    if duplicate.any():
        sample = selected.loc[duplicate, [*_KEY_COLUMNS]].iloc[0]
        raise ValueError(
            f"{source_name} contains duplicate curve points for "
            f"ref_date={sample['ref_date']:%Y-%m-%d}, "
            f"tenor_bd={sample['tenor_bd']}"
        )

    selected[rate_column] = pd.to_numeric(
        selected[rate_column], errors="coerce"
    ).astype(np.float64)
    matrix = selected.pivot(
        index="ref_date",
        columns="tenor_bd",
        values=rate_column,
    )
    matrix = matrix.sort_index().sort_index(axis="columns")
    matrix.columns = matrix.columns.astype(str)
    matrix.columns.name = None
    matrix.index.name = "ref_date"
    return matrix


def format_partitioned_curves(
    partitions: Mapping[str, CurvePartition],
    *,
    rate_column: str = "fitted_rate",
    source_name: str,
) -> pd.DataFrame:
    """Load and pivot batched curve partitions without concatenating them long."""

    if not partitions:
        raise ValueError(f"{source_name} contains no curve partitions")

    matrices: list[pd.DataFrame] = []
    expected_columns: pd.Index | None = None
    for partition_id, partition in sorted(partitions.items()):
        frame = partition() if callable(partition) else partition
        if not isinstance(frame, pd.DataFrame):
            raise TypeError(
                f"{source_name} partition {partition_id!r} did not load "
                "as a pandas DataFrame"
            )
        matrix = _validate_and_pivot_curve(
            frame,
            rate_column=rate_column,
            source_name=f"{source_name}[{partition_id}]",
        )
        if expected_columns is None:
            expected_columns = matrix.columns
        elif not matrix.columns.equals(expected_columns):
            raise ValueError(
                f"{source_name} partition {partition_id!r} has a different "
                "business-day grid"
            )
        matrices.append(matrix)

    result = pd.concat(matrices, axis=0).sort_index()
    if result.index.has_duplicates:
        duplicate_date = result.index[result.index.duplicated()][0]
        raise ValueError(
            f"{source_name} contains ref_date {duplicate_date:%Y-%m-%d} "
            "in more than one partition"
        )
    return result


def data_treatment(
    flat_forward_curves: pd.DataFrame,
    nelson_siegel_curves: Mapping[str, CurvePartition],
    svensson_curves: Mapping[str, CurvePartition],
    kernel_ridge_curves: Mapping[str, CurvePartition],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, bool]:
    """Create one wide, test-ready DataFrame for each curve methodology."""

    flat_forward = _validate_and_pivot_curve(
        flat_forward_curves,
        rate_column="zero_rate",
        source_name="flat_forward",
    )
    nelson_siegel = format_partitioned_curves(
        nelson_siegel_curves,
        source_name="nelson_siegel",
    )
    svensson = format_partitioned_curves(
        svensson_curves,
        source_name="svensson",
    )
    kernel_ridge = format_partitioned_curves(
        kernel_ridge_curves,
        source_name="kernel_ridge",
    )
    return flat_forward, nelson_siegel, svensson, kernel_ridge, True


def register_curve_duckdb_views(
    treatment_complete: bool,
    duckdb_path: str,
    views: Mapping[str, str],
) -> None:
    """Register persistent DuckDB views over the treated curve Parquets."""

    if treatment_complete is not True:
        raise ValueError("Curve data treatment did not complete successfully")
    if not views:
        raise ValueError("No curve DuckDB views were configured")

    database = Path(duckdb_path)
    database.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database)) as connection:
        for view_name, parquet_path in views.items():
            if not _VIEW_NAME_PATTERN.fullmatch(view_name):
                raise ValueError(f"Invalid DuckDB view name: {view_name!r}")
            resolved_path = Path(parquet_path).resolve()
            if not resolved_path.is_file():
                raise FileNotFoundError(
                    f"Curve Parquet does not exist: {resolved_path}"
                )
            escaped_path = resolved_path.as_posix().replace("'", "''")
            connection.execute(
                f'CREATE OR REPLACE VIEW "{view_name}" AS '
                f"SELECT * FROM read_parquet('{escaped_path}')"
            )
