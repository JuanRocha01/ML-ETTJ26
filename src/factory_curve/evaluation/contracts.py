from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import pandas as pd

from .curves import DailyCurveMatrix


@dataclass(frozen=True)
class EvaluationContext:
    """Immutable inputs shared by independent metric calculators."""

    methodology: str
    curve: DailyCurveMatrix
    ltn_observations: pd.DataFrame
    swap_observations: pd.DataFrame
    calendar: pd.DataFrame
    parameters: dict[str, Any]


class MetricCalculator(Protocol):
    """Extension point implemented by each evaluation family."""

    @property
    def result_keys(self) -> tuple[str, ...]:
        """Names of the result tables returned by the calculator."""

    def calculate(
        self,
        context: EvaluationContext,
    ) -> dict[str, pd.DataFrame]:
        """Calculate one independent family of metrics."""
