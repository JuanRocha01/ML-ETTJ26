from __future__ import annotations

import numpy as np
import pytest

from factory_curve.svensson.model import (
    SvenssonSpecification,
    svensson_loadings,
)


def test_svensson_loadings_add_a_second_curvature_column() -> None:
    matrix = svensson_loadings([0.0, 1.0, 2.0], 0.8, 0.2)

    assert matrix.shape == (3, 4)
    assert matrix[0] == pytest.approx([1.0, 1.0, 0.0, 0.0])
    assert np.isfinite(matrix).all()


def test_svensson_rejects_label_switching_and_close_lambdas() -> None:
    specification = SvenssonSpecification(min_lambda_ratio=1.2)

    assert specification.validate_lambdas([0.8, 0.2])
    assert not specification.validate_lambdas([0.2, 0.8])
    assert not specification.validate_lambdas([0.22, 0.2])


def test_svensson_ratio_must_be_greater_than_one() -> None:
    with pytest.raises(ValueError, match="greater than one"):
        SvenssonSpecification(min_lambda_ratio=1.0)
