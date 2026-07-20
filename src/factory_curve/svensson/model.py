from __future__ import annotations

from typing import Sequence

import numpy as np

from factory_curve.parametric.loadings import slope_and_curvature_loadings


def svensson_loadings(
    tenors: Sequence[float] | np.ndarray,
    lambda_1: float,
    lambda_2: float,
) -> np.ndarray:
    slope, first_curvature = slope_and_curvature_loadings(tenors, lambda_1)
    _, second_curvature = slope_and_curvature_loadings(tenors, lambda_2)
    return np.column_stack(
        (
            np.ones_like(slope),
            slope,
            first_curvature,
            second_curvature,
        )
    )


class SvenssonSpecification:
    """
    Svensson matrix with a stable factor identity.

    ``lambda_1 / lambda_2 >= min_lambda_ratio`` makes factor one the
    short/intermediate curvature and factor two the long curvature, preventing
    daily label switching and near-collinear curvature columns.
    """

    name = "svensson"
    lambda_names = ("lambda_1", "lambda_2")
    beta_names = ("beta_0", "beta_1", "beta_2", "beta_3")

    def __init__(self, min_lambda_ratio: float = 1.2) -> None:
        if min_lambda_ratio <= 1.0:
            raise ValueError("min_lambda_ratio must be greater than one")
        self.min_lambda_ratio = float(min_lambda_ratio)

    def design_matrix(
        self,
        tenors: Sequence[float] | np.ndarray,
        lambdas: Sequence[float] | np.ndarray,
    ) -> np.ndarray:
        lambda_values = np.asarray(lambdas, dtype=np.float64)
        if lambda_values.shape != (2,):
            raise ValueError("Svensson requires exactly two lambdas")
        return svensson_loadings(
            tenors,
            float(lambda_values[0]),
            float(lambda_values[1]),
        )

    def validate_lambdas(
        self,
        lambdas: Sequence[float] | np.ndarray,
    ) -> bool:
        values = np.asarray(lambdas, dtype=np.float64)
        return bool(
            values.shape == (2,)
            and np.isfinite(values).all()
            and (values > 0.0).all()
            and values[0] / values[1] >= self.min_lambda_ratio
        )
