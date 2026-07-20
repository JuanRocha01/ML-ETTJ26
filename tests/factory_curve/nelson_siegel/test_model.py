from __future__ import annotations

import numpy as np
import pytest

from factory_curve.nelson_siegel.model import (
    NelsonSiegelSpecification,
    nelson_siegel_loadings,
)


def test_nelson_siegel_loadings_have_level_slope_and_curvature_columns() -> None:
    matrix = nelson_siegel_loadings([0.0, 1.0, 2.0], lambda_1=0.5)

    assert matrix.shape == (3, 3)
    assert matrix[:, 0] == pytest.approx(np.ones(3))
    assert matrix[0] == pytest.approx([1.0, 1.0, 0.0])
    expected_slope = (1.0 - np.exp(-0.5)) / 0.5
    assert matrix[1, 1] == pytest.approx(expected_slope)
    assert matrix[1, 2] == pytest.approx(expected_slope - np.exp(-0.5))


def test_nelson_siegel_loadings_are_stable_at_tiny_tenors() -> None:
    matrix = nelson_siegel_loadings([1e-14], lambda_1=0.7)

    assert np.isfinite(matrix).all()
    assert matrix[0, 1] == pytest.approx(1.0)
    assert matrix[0, 2] == pytest.approx(0.0, abs=1e-12)


def test_nelson_siegel_requires_one_positive_lambda() -> None:
    specification = NelsonSiegelSpecification()

    assert specification.validate_lambdas([0.5])
    assert not specification.validate_lambdas([0.0])
    assert not specification.validate_lambdas([0.5, 0.2])
