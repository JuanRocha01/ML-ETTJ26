from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

import numpy as np
import pandas as pd


def _positive_finite_vector(
    values: Sequence[float] | np.ndarray,
    *,
    name: str,
) -> np.ndarray:
    result = np.asarray(values, dtype=np.float64)
    if result.ndim != 1 or result.size == 0:
        raise ValueError(f"{name} must be a non-empty one-dimensional array")
    if not np.isfinite(result).all() or (result <= 0.0).any():
        raise ValueError(f"{name} must contain finite, strictly positive values")
    return result


def kernel_matrix(
    x: Sequence[float] | np.ndarray,
    y: Sequence[float] | np.ndarray,
    *,
    alpha: float,
    delta: float,
) -> np.ndarray:
    """
    Return the Filipović-Pelger-Ye RKHS kernel on positive year tenors.

    The implementation follows ``yye9701/KR_example`` but evaluates arbitrary
    tenor vectors instead of allocating a dense daily square matrix.
    """

    x_values = _positive_finite_vector(x, name="x")
    y_values = _positive_finite_vector(y, name="y")
    alpha_value = float(alpha)
    delta_value = float(delta)
    if not np.isfinite(alpha_value) or alpha_value <= 0.0:
        raise ValueError("alpha must be finite and strictly positive")
    if not np.isfinite(delta_value) or not 0.0 <= delta_value <= 1.0:
        raise ValueError("delta must be finite and between zero and one")

    x_grid = x_values[:, None]
    y_grid = y_values[None, :]
    minimum = np.minimum(x_grid, y_grid)
    maximum = np.maximum(x_grid, y_grid)

    if delta_value == 0.0:
        return (
            -(minimum / alpha_value**2) * np.exp(-alpha_value * minimum)
            + (2.0 / alpha_value**3)
            * (1.0 - np.exp(-alpha_value * minimum))
            - (minimum / alpha_value**2) * np.exp(-alpha_value * maximum)
        )

    if delta_value == 1.0:
        return (
            1.0
            / alpha_value
            * (1.0 - np.exp(-alpha_value * minimum))
        )

    sqrt_discriminant = np.sqrt(
        alpha_value**2 + 4.0 * delta_value / (1.0 - delta_value)
    )
    lambda_1 = (alpha_value - sqrt_discriminant) / 2.0
    lambda_2 = (alpha_value + sqrt_discriminant) / 2.0
    return (
        -alpha_value
        / (delta_value * lambda_2**2)
        * (
            1.0
            - np.exp(-lambda_2 * x_grid)
            - np.exp(-lambda_2 * y_grid)
        )
        + 1.0
        / (alpha_value * delta_value)
        * (1.0 - np.exp(-alpha_value * minimum))
        + 1.0
        / (delta_value * sqrt_discriminant)
        * (
            lambda_1**2
            / lambda_2**2
            * np.exp(-lambda_2 * (x_grid + y_grid))
            - np.exp(-lambda_1 * minimum - lambda_2 * maximum)
        )
    )


@dataclass(frozen=True)
class KernelRidgeDailyModel:
    """Serializable coefficients and diagnostics for one daily KR curve."""

    reference_date: str
    alpha: float
    delta: float
    ridge: float
    business_days_per_year: int
    cashflow_tenors_bd: np.ndarray
    coefficients: np.ndarray
    n_observations: int
    max_cashflow_bd: int
    price_rmse: float
    weighted_yield_rmse_approx: float
    max_abs_price_error: float
    condition_number: float
    source_isins: tuple[str, ...]

    def __post_init__(self) -> None:
        tenors = np.asarray(self.cashflow_tenors_bd, dtype=np.int64)
        coefficients = np.asarray(self.coefficients, dtype=np.float64)
        if tenors.ndim != 1 or tenors.size == 0 or (tenors <= 0).any():
            raise ValueError("cashflow_tenors_bd must contain positive tenors")
        if coefficients.shape != tenors.shape:
            raise ValueError(
                "coefficients and cashflow_tenors_bd must have identical shapes"
            )
        if not np.isfinite(coefficients).all():
            raise ValueError("coefficients must be finite")
        if self.business_days_per_year <= 0:
            raise ValueError("business_days_per_year must be strictly positive")
        object.__setattr__(self, "cashflow_tenors_bd", tenors)
        object.__setattr__(self, "coefficients", coefficients)

    @property
    def cashflow_tenors_years(self) -> np.ndarray:
        return (
            self.cashflow_tenors_bd.astype(np.float64)
            / self.business_days_per_year
        )

    def discount_factors(
        self,
        tenor_bd: Sequence[int] | np.ndarray,
    ) -> np.ndarray:
        tenor_values = np.asarray(tenor_bd, dtype=np.int64)
        if tenor_values.ndim != 1 or tenor_values.size == 0:
            raise ValueError("tenor_bd must be a non-empty vector")
        if (tenor_values <= 0).any():
            raise ValueError("tenor_bd must be strictly positive")
        tenor_years = (
            tenor_values.astype(np.float64) / self.business_days_per_year
        )
        cross_kernel = kernel_matrix(
            tenor_years,
            self.cashflow_tenors_years,
            alpha=self.alpha,
            delta=self.delta,
        )
        return 1.0 + cross_kernel @ self.coefficients

    def curve_frame(self, *, max_years: int) -> pd.DataFrame:
        if max_years <= 0:
            raise ValueError("max_years must be strictly positive")
        tenor_bd = np.arange(
            1,
            max_years * self.business_days_per_year + 1,
            dtype=np.int32,
        )
        tenor_years = (
            tenor_bd.astype(np.float64) / self.business_days_per_year
        )
        discount_factors = self.discount_factors(tenor_bd)
        valid = np.isfinite(discount_factors) & (discount_factors > 0.0)
        log_yield = np.full(discount_factors.shape, np.nan, dtype=np.float64)
        fitted_rate = np.full(discount_factors.shape, np.nan, dtype=np.float64)
        log_yield[valid] = -np.log(discount_factors[valid]) / tenor_years[valid]
        fitted_rate[valid] = np.expm1(log_yield[valid])
        return pd.DataFrame(
            {
                "ref_date": pd.Timestamp(self.reference_date),
                "tenor_bd": tenor_bd,
                "tenor_years": tenor_years,
                "discount_factor": discount_factors,
                "log_yield": log_yield,
                "fitted_rate": fitted_rate,
                "is_valid_discount_factor": valid,
            }
        )
