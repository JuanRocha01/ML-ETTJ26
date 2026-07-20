from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

from factory_curve.evaluation.contracts import EvaluationContext
from factory_curve.evaluation.curves import DailyCurveMatrix
from factory_curve.evaluation.forwards import ForwardArbitrageCalculator
from factory_curve.evaluation.pca import DailyPCACalculator
from factory_curve.evaluation.rolldown import RolldownCalculator


def _context(sample, curve=None) -> EvaluationContext:
    return EvaluationContext(
        methodology="test",
        curve=DailyCurveMatrix.from_frame(
            sample["curve"] if curve is None else curve
        ),
        ltn_observations=sample["ltn"],
        swap_observations=sample["swaps"],
        calendar=sample["calendar"],
        parameters=sample["parameters"],
    )


def test_pca_returns_daily_scores_and_named_three_factor_loadings(
    evaluation_sample,
) -> None:
    result = DailyPCACalculator().calculate(_context(evaluation_sample))
    scores = result["pca_scores_daily"]
    loadings = result["pca_loadings"]
    assert set(scores["factor"]) == {"level", "slope", "curvature"}
    assert scores["ref_date"].nunique() == 9
    assert set(loadings["factor"]) == {"level", "slope", "curvature"}
    explained = (
        scores.drop_duplicates("factor")["explained_variance_ratio"].sum()
    )
    assert explained == pytest.approx(1.0)


def test_forward_test_flags_negative_implied_forward(
    evaluation_sample,
) -> None:
    curve = evaluation_sample["curve"].copy()
    curve.loc[:, "2"] = -0.50
    result = ForwardArbitrageCalculator().calculate(
        _context(evaluation_sample, curve)
    )
    violations = result["forward_violations"]
    assert not violations.empty
    assert "below_minimum" in set(violations["violation_reason"])


def test_forward_test_handles_invalid_rates_and_overflow_in_log_domain(
    evaluation_sample,
) -> None:
    curve = pd.DataFrame(
        [[0.05, 1.0e308, -1.1]],
        index=[pd.Timestamp("2020-01-02")],
        columns=["1", "2", "3"],
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        result = ForwardArbitrageCalculator().calculate(
            _context(evaluation_sample, curve)
        )
    violations = result["forward_violations"]
    assert {"invalid_forward", "above_maximum"}.issubset(
        set(violations["violation_reason"])
    )
    above = violations.loc[
        violations["violation_reason"].eq("above_maximum")
    ].iloc[0]
    assert np.isnan(above["implied_forward_rate"])
    assert above["annualized_log_forward"] > 700.0


def test_rolldown_selects_one_short_two_medium_and_one_long_point(
    evaluation_sample,
) -> None:
    outputs = RolldownCalculator().calculate(_context(evaluation_sample))
    result = outputs["rolldown_results"]
    assert len(result) == 4
    assert result["tenor_segment"].value_counts().to_dict() == {
        "medium": 2,
        "short": 1,
        "long": 1,
    }
    assert result["predicted_rate_d1_direct"].notna().all()
    assert result["predicted_rate_d1_taylor"].notna().all()
    assert result["price_first_derivative"].lt(0.0).all()
    assert len(outputs["rolldown_metrics"]) == 6
