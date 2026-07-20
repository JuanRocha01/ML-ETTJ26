from __future__ import annotations

from typing import Any

import pandas as pd
from statsmodels.regression.linear_model import RegressionResultsWrapper

from factory_curve.parametric.core import CurveFitConfig, fit_models_by_date

from .model import SvenssonSpecification


def fit_svensson_models(
    curve_inputs: pd.DataFrame,
    parameters: dict[str, Any],
) -> dict[str, RegressionResultsWrapper]:
    """Fit one independent Svensson Statsmodels WLS result per date."""

    config = CurveFitConfig.from_mapping(
        parameters,
        expected_lambda_count=2,
        default_min_observations=5,
    )
    specification = SvenssonSpecification(
        min_lambda_ratio=float(parameters.get("min_lambda_ratio", 1.2))
    )
    return fit_models_by_date(
        curve_inputs,
        specification=specification,
        config=config,
    )
