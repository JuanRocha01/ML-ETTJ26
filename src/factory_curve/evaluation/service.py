from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from .contracts import EvaluationContext, MetricCalculator
from .curves import DailyCurveMatrix
from .forwards import ForwardArbitrageCalculator
from .pca import DailyPCACalculator
from .rate_fit import RateFitCalculator
from .repricing import RepricingCalculator
from .rolldown import RolldownCalculator


DEFAULT_CALCULATORS: tuple[MetricCalculator, ...] = (
    RateFitCalculator(),
    RepricingCalculator(),
    DailyPCACalculator(),
    ForwardArbitrageCalculator(),
    RolldownCalculator(),
)


class CurveEvaluationService:
    """Open/closed orchestrator over injected metric calculators."""

    def __init__(
        self,
        calculators: Sequence[MetricCalculator] = DEFAULT_CALCULATORS,
    ) -> None:
        self._calculators = tuple(calculators)
        keys = [
            key
            for calculator in self._calculators
            for key in calculator.result_keys
        ]
        if len(keys) != len(set(keys)):
            raise ValueError("Metric calculators expose duplicate result keys")
        self._result_keys = tuple(keys)

    @property
    def result_keys(self) -> tuple[str, ...]:
        return self._result_keys

    def evaluate(
        self,
        curves: Mapping[str, pd.DataFrame],
        *,
        ltn_observations: pd.DataFrame,
        swap_observations: pd.DataFrame,
        calendar: pd.DataFrame,
        parameters: dict[str, Any],
    ) -> dict[str, pd.DataFrame]:
        collected = {key: [] for key in self._result_keys}
        for methodology, frame in curves.items():
            context = EvaluationContext(
                methodology=methodology,
                curve=DailyCurveMatrix.from_frame(frame),
                ltn_observations=ltn_observations,
                swap_observations=swap_observations,
                calendar=calendar,
                parameters=parameters,
            )
            for calculator in self._calculators:
                results = calculator.calculate(context)
                if set(results) != set(calculator.result_keys):
                    raise ValueError(
                        f"{type(calculator).__name__} returned an invalid "
                        "result contract"
                    )
                for key, result in results.items():
                    collected[key].append(result)

        return {
            key: pd.concat(frames, ignore_index=True)
            if frames
            else pd.DataFrame()
            for key, frames in collected.items()
        }
