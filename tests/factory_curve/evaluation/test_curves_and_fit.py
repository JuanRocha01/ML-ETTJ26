from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from factory_curve.evaluation.contracts import EvaluationContext
from factory_curve.evaluation.curves import DailyCurveMatrix
from factory_curve.evaluation.rate_fit import RateFitCalculator
from factory_curve.evaluation.repricing import (
    RepricingCalculator,
    zero_coupon_price,
)


def _context(sample) -> EvaluationContext:
    return EvaluationContext(
        methodology="test",
        curve=DailyCurveMatrix.from_frame(sample["curve"]),
        ltn_observations=sample["ltn"],
        swap_observations=sample["swaps"],
        calendar=sample["calendar"],
        parameters=sample["parameters"],
    )


def test_curve_matrix_vectorized_lookup(evaluation_sample) -> None:
    matrix = DailyCurveMatrix.from_frame(evaluation_sample["curve"])
    rates = matrix.lookup(
        ["2020-01-02", "2020-01-03", "1999-01-01"],
        [5, 4, 5],
    )
    assert rates[:2] == pytest.approx([0.0505, 0.0506])
    assert np.isnan(rates[2])


def test_rate_fit_has_zero_rmse_for_exact_ltn_and_swap_points(
    evaluation_sample,
) -> None:
    result = RateFitCalculator().calculate(_context(evaluation_sample))
    metrics = result["rate_metrics_daily"]
    assert set(metrics["sample"]) == {"in_sample", "out_of_sample"}
    assert metrics["rmse"].max() == pytest.approx(0.0, abs=1e-15)
    summary = result["rate_metrics_summary"]
    assert len(summary) == 2
    assert summary["rmse"].max() == pytest.approx(0.0, abs=1e-15)


def test_repricing_uses_market_ltn_pu_and_swap_theoretical_price(
    evaluation_sample,
) -> None:
    result = RepricingCalculator().calculate(_context(evaluation_sample))
    errors = result["repricing_errors"]
    assert errors["price_error"].abs().max() == pytest.approx(
        0.0, abs=1e-12
    )
    swap = errors.loc[errors["sample"].eq("out_of_sample")].iloc[0]
    expected = 1000.0 / (1.0 + 0.0505) ** (5.0 / 252.0)
    assert swap["observed_price"] == pytest.approx(expected)
    assert len(result["repricing_metrics_summary"]) == 2


def test_curve_matrix_rejects_non_tenor_columns() -> None:
    frame = pd.DataFrame({"bad": [0.1]}, index=["2020-01-02"])
    with pytest.raises(ValueError, match="business-day integer labels"):
        DailyCurveMatrix.from_frame(frame)


def test_zero_coupon_price_rejects_invalid_domain_without_warning() -> None:
    with np.errstate(all="raise"):
        prices = zero_coupon_price(
            np.asarray([-1.1, 0.05]),
            np.asarray([5040, 252]),
            notional=1000.0,
            business_days_per_year=252,
        )
    assert np.isnan(prices[0])
    assert prices[1] == pytest.approx(1000.0 / 1.05)
