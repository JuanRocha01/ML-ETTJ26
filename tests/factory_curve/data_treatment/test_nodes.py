from __future__ import annotations

import duckdb
import pandas as pd
import pytest

from factory_curve.data_treatment.nodes import (
    data_treatment,
    format_partitioned_curves,
    register_curve_duckdb_views,
)


def _curve_frame(
    dates: list[str],
    *,
    rate_column: str = "fitted_rate",
) -> pd.DataFrame:
    rows = []
    for date_index, ref_date in enumerate(dates):
        for tenor_bd in (1, 2, 3):
            rows.append(
                {
                    "ref_date": ref_date,
                    "tenor_bd": tenor_bd,
                    rate_column: date_index + tenor_bd / 100,
                }
            )
    return pd.DataFrame(rows)


def test_data_treatment_pivots_all_methodologies_and_loads_batches() -> None:
    load_count = {"count": 0}

    def lazy_partition() -> pd.DataFrame:
        load_count["count"] += 1
        return _curve_frame(["2020-01-03"])

    partitions = {
        "batch_00001": lazy_partition,
        "batch_00000": _curve_frame(["2020-01-02"]),
    }
    outputs = data_treatment(
        flat_forward_curves=_curve_frame(
            ["2020-01-02", "2020-01-03"],
            rate_column="zero_rate",
        ),
        bootstrapping_curves=_curve_frame(
            ["2020-01-02", "2020-01-03"],
            rate_column="zero_rate",
        ),
        nelson_siegel_curves=partitions,
        svensson_curves=partitions,
        kernel_ridge_curves=partitions,
    )

    for matrix in outputs[:5]:
        assert matrix.index.name == "ref_date"
        assert list(matrix.columns) == ["1", "2", "3"]
        assert list(matrix.index) == [
            pd.Timestamp("2020-01-02"),
            pd.Timestamp("2020-01-03"),
        ]
    assert load_count["count"] == 3
    assert outputs[5] is True


def test_partitioned_curves_reject_different_business_day_grids() -> None:
    incomplete = _curve_frame(["2020-01-03"]).query("tenor_bd != 3")
    with pytest.raises(ValueError, match="different business-day grid"):
        format_partitioned_curves(
            {
                "batch_00000": _curve_frame(["2020-01-02"]),
                "batch_00001": incomplete,
            },
            source_name="test_curve",
        )


def test_register_curve_duckdb_views_reads_saved_index(
    tmp_path,
) -> None:
    matrix = format_partitioned_curves(
        {"batch": _curve_frame(["2020-01-02"])},
        source_name="test_curve",
    )
    parquet_path = tmp_path / "curve.parquet"
    database_path = tmp_path / "curves.duckdb"
    matrix.to_parquet(parquet_path, index=True)

    register_curve_duckdb_views(
        True,
        str(database_path),
        {"vw_test_curve": str(parquet_path)},
    )

    with duckdb.connect(str(database_path), read_only=True) as connection:
        result = connection.sql(
            'SELECT ref_date, "1", "3" FROM vw_test_curve'
        ).df()
    assert result.to_dict("records") == [
        {
            "ref_date": pd.Timestamp("2020-01-02"),
            "1": 0.01,
            "3": 0.03,
        }
    ]
