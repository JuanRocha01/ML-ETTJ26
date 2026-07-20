from __future__ import annotations

from factory_curve.parametric.calculator import (
    CurveCalculationConfig,
    ParametricCurveCalculator,
)

from .model import SvenssonSpecification


class SvenssonCurveCalculator(ParametricCurveCalculator):
    """Calculate Svensson grids from persisted daily models."""

    def __init__(
        self,
        config: CurveCalculationConfig | None = None,
    ) -> None:
        super().__init__(
            specification=SvenssonSpecification(),
            config=config,
        )
