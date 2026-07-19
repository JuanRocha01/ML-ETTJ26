from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from factory_curve.bootstrapping.interpolation import (
    FlatForwardConfig,
    FlatForwardInterpolator,
    PublicBondCurveBatchBuilder,
    interpolate_flat_forward,
)


def test_interpolation_preserves_observed_tenor_rates() -> None:
    curve = interpolate_flat_forward(
        tenors=[1.0, 2.0],
        rates=[0.10, 0.21],
        max_years=2,
        business_days_per_year=2,
    )

    assert curve["tenor_bd"].tolist() == [1, 2, 3, 4]
    assert curve.loc[curve["tenor_years"].eq(1.0), "zero_rate"].item() == pytest.approx(
        0.10
    )
    assert curve.loc[curve["tenor_years"].eq(2.0), "zero_rate"].item() == pytest.approx(
        0.21
    )


def test_log_discount_factors_are_linear_inside_each_segment() -> None:
    curve = interpolate_flat_forward(
        tenors=[1.0, 2.0],
        rates=[0.10, 0.20],
        max_years=2,
        business_days_per_year=2,
    )

    log_df_at_one = -np.log1p(0.10)
    log_df_at_two = -2.0 * np.log1p(0.20)
    expected_midpoint = (log_df_at_one + log_df_at_two) / 2.0

    actual_midpoint = np.log(
        curve.loc[curve["tenor_years"].eq(1.5), "discount_factor"].item()
    )
    assert actual_midpoint == pytest.approx(expected_midpoint)

    segment_forwards = curve.loc[
        curve["tenor_years"].isin([1.5, 2.0]), "forward_rate"
    ]
    assert segment_forwards.nunique() == 1


def test_extrapolation_is_flat_forward_before_and_after_observed_tenors() -> None:
    curve = interpolate_flat_forward(
        tenors=[1.0, 2.0],
        rates=[0.10, 0.20],
        max_years=3,
        business_days_per_year=2,
    )

    short_end = curve.loc[curve["tenor_years"].le(1.0), "forward_rate"]
    long_end = curve.loc[curve["tenor_years"].ge(1.5), "forward_rate"]

    assert short_end.nunique() == 1
    assert short_end.iloc[0] == pytest.approx(0.10)
    assert long_end.nunique() == 1


def test_unsorted_and_duplicate_tenors_are_normalized_in_discount_space() -> None:
    interpolator = FlatForwardInterpolator(
        FlatForwardConfig(max_years=2, business_days_per_year=1)
    )
    curve = interpolator.interpolate(
        tenors=[2.0, 1.0, 1.0],
        rates=[0.20, 0.10, 0.21],
    )

    expected_log_df = np.mean([-np.log1p(0.10), -np.log1p(0.21)])
    expected_rate = np.exp(-expected_log_df) - 1.0

    assert curve.loc[curve["tenor_years"].eq(1.0), "zero_rate"].item() == pytest.approx(
        expected_rate
    )
    assert curve.loc[curve["tenor_years"].eq(2.0), "zero_rate"].item() == pytest.approx(
        0.20
    )


def test_nearly_identical_durations_share_the_same_business_day_vertex() -> None:
    curve = interpolate_flat_forward(
        tenors=[1.0, 1.0 + 1.0e-12, 2.0],
        rates=[0.10, 0.20, 0.15],
        max_years=2,
        business_days_per_year=252,
    )

    assert np.isfinite(curve.select_dtypes(include="number").to_numpy()).all()


@pytest.mark.parametrize(
    ("tenors", "rates", "message"),
    [
        ([], [], "at least one"),
        ([1.0], [0.10, 0.20], "same length"),
        ([0.0], [0.10], "strictly positive"),
        ([1.0], [-1.0], "greater than -1"),
        ([np.nan], [0.10], "finite"),
    ],
)
def test_invalid_curve_inputs_raise_clear_errors(
    tenors: list[float],
    rates: list[float],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        interpolate_flat_forward(tenors=tenors, rates=rates)


def test_batch_builder_filters_dates_and_builds_one_curve_per_reference_date() -> None:
    curve_inputs = pd.DataFrame(
        {
            "ref_date": [
                "2019-12-31",
                "2020-01-02",
                "2020-01-02",
                "2020-01-02",
                "2020-01-03",
            ],
            "instrument_type": ["LTN", "LTN", "NTN-F", "LTN", "NTN-F"],
            "macaulay_duration": [1.0, 1.0, 1.0, 2.0, 1.5],
            "market_ytm": [0.09, 0.10, 0.21, 0.20, 0.15],
        }
    )
    builder = PublicBondCurveBatchBuilder(
        interpolator=FlatForwardInterpolator(
            FlatForwardConfig(max_years=3, business_days_per_year=1)
        ),
        start_date="2020-01-01",
    )

    result = builder.build(curve_inputs)

    assert result["ref_date"].dt.strftime("%Y-%m-%d").unique().tolist() == [
        "2020-01-02",
        "2020-01-03",
    ]
    assert len(result) == 6
    assert result.groupby("ref_date")["tenor_bd"].apply(list).tolist() == [
        [1, 2, 3],
        [1, 2, 3],
    ]
    assert result.groupby("ref_date")["source_tenor_count"].first().tolist() == [2, 1]


def test_batch_builder_rejects_missing_columns() -> None:
    builder = PublicBondCurveBatchBuilder()

    with pytest.raises(ValueError, match="Missing required columns"):
        builder.build(pd.DataFrame({"ref_date": ["2020-01-02"]}))
