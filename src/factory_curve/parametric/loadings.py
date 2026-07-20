from __future__ import annotations

from typing import Sequence

import numpy as np


def slope_and_curvature_loadings(
    tenors: Sequence[float] | np.ndarray,
    decay: float,
) -> tuple[np.ndarray, np.ndarray]:
    """Compute stable Nelson-Siegel slope and curvature loadings."""

    tenor_values = np.asarray(tenors, dtype=np.float64)
    decay_value = float(decay)
    if tenor_values.ndim != 1:
        raise ValueError("Tenors must be one-dimensional")
    if not np.isfinite(tenor_values).all() or (tenor_values < 0.0).any():
        raise ValueError("Tenors must be finite and non-negative")
    if not np.isfinite(decay_value) or decay_value <= 0.0:
        raise ValueError("Decay must be finite and strictly positive")

    scaled_tenor = decay_value * tenor_values
    slope = np.empty_like(scaled_tenor)
    near_zero = np.abs(scaled_tenor) < 1.0e-7
    x = scaled_tenor[near_zero]
    slope[near_zero] = 1.0 - x / 2.0 + x * x / 6.0 - x * x * x / 24.0
    slope[~near_zero] = (
        -np.expm1(-scaled_tenor[~near_zero]) / scaled_tenor[~near_zero]
    )
    curvature = slope - np.exp(-scaled_tenor)
    return slope, curvature
