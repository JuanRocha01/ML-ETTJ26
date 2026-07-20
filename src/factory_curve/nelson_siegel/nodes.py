from __future__ import annotations

from typing import Any

import pandas as pd
from statsmodels.regression.linear_model import RegressionResultsWrapper

from factory_curve.parametric.core import CurveFitConfig, fit_models_by_date

from .model import NelsonSiegelSpecification


def fit_nelson_siegel_models(
    curve_inputs: pd.DataFrame,
    parameters: dict[str, Any],
) -> dict[str, RegressionResultsWrapper]:
    """Fit one independent Nelson-Siegel Statsmodels WLS result per date."""

    config = CurveFitConfig.from_mapping(
        parameters,
        expected_lambda_count=1,
        default_min_observations=4,
    )
    return fit_models_by_date(
        curve_inputs,
        specification=NelsonSiegelSpecification(),
        config=config,
    )
