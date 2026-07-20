from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import pandas as pd
from statsmodels.regression.linear_model import RegressionResultsWrapper

from factory_curve.parametric.calculator import (
    CurveBatchPartitionBuilder,
    CurveCalculationConfig,
    ModelDimensionBuilder,
)

from .calculator import SvenssonCurveCalculator


ModelPartitions = Mapping[
    str,
    Callable[[], RegressionResultsWrapper] | RegressionResultsWrapper,
]


def build_svensson_parameter_dimension(
    model_partitions: ModelPartitions,
    parameters: dict[str, Any],
) -> pd.DataFrame:
    config = CurveCalculationConfig.from_mapping(parameters)
    calculator = SvenssonCurveCalculator(config)
    return ModelDimensionBuilder(calculator).build(model_partitions)


def build_svensson_curve_batches(
    model_partitions: ModelPartitions,
    parameters: dict[str, Any],
) -> dict[str, Callable[[], pd.DataFrame]]:
    config = CurveCalculationConfig.from_mapping(parameters)
    calculator = SvenssonCurveCalculator(config)
    return CurveBatchPartitionBuilder(calculator).build(model_partitions)
