from __future__ import annotations

from factory_curve.parametric.calculator import (
    CurveCalculationConfig,
    ParametricCurveCalculator,
)

from .model import NelsonSiegelSpecification


class NelsonSiegelCurveCalculator(ParametricCurveCalculator):
    """Calculate Nelson-Siegel grids from persisted daily models."""

    def __init__(
        self,
        config: CurveCalculationConfig | None = None,
    ) -> None:
        super().__init__(
            specification=NelsonSiegelSpecification(),
            config=config,
        )
