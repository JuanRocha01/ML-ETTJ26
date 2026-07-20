from __future__ import annotations

from typing import Sequence

import numpy as np

from factory_curve.parametric.loadings import slope_and_curvature_loadings


def nelson_siegel_loadings(
    tenors: Sequence[float] | np.ndarray,
    lambda_1: float,
) -> np.ndarray:
    slope, curvature = slope_and_curvature_loadings(tenors, lambda_1)
    return np.column_stack((np.ones_like(slope), slope, curvature))


class NelsonSiegelSpecification:
    """Nelson-Siegel design matrix conditional on one positive lambda."""

    name = "nelson_siegel"
    lambda_names = ("lambda_1",)
    beta_names = ("beta_0", "beta_1", "beta_2")

    def design_matrix(
        self,
        tenors: Sequence[float] | np.ndarray,
        lambdas: Sequence[float] | np.ndarray,
    ) -> np.ndarray:
        lambda_values = np.asarray(lambdas, dtype=np.float64)
        if lambda_values.shape != (1,):
            raise ValueError("Nelson-Siegel requires exactly one lambda")
        return nelson_siegel_loadings(tenors, float(lambda_values[0]))

    def validate_lambdas(
        self,
        lambdas: Sequence[float] | np.ndarray,
    ) -> bool:
        values = np.asarray(lambdas, dtype=np.float64)
        return bool(
            values.shape == (1,)
            and np.isfinite(values).all()
            and (values > 0.0).all()
        )
