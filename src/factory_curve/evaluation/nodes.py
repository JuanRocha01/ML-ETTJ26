from __future__ import annotations

from typing import Any

import pandas as pd

from .service import CurveEvaluationService


OUTPUT_KEYS = (
    "rate_errors",
    "rate_metrics_daily",
    "rate_metrics_summary",
    "repricing_errors",
    "repricing_metrics_daily",
    "repricing_metrics_summary",
    "pca_scores_daily",
    "pca_loadings",
    "forward_diagnostics_daily",
    "forward_violations",
    "rolldown_results",
    "rolldown_metrics",
)


def evaluate_curve_methodologies(
    flat_forward_curve: pd.DataFrame,
    bootstrapping_curve: pd.DataFrame,
    nelson_siegel_curve: pd.DataFrame,
    svensson_curve: pd.DataFrame,
    kernel_ridge_curve: pd.DataFrame,
    curve_inputs: pd.DataFrame,
    swap_observations: pd.DataFrame,
    calendar: pd.DataFrame,
    parameters: dict[str, Any],
) -> tuple[pd.DataFrame, ...]:
    """Run every configured metric for all curve methodologies."""

    service = CurveEvaluationService()
    results = service.evaluate(
        {
            "flat_forward": flat_forward_curve,
            "bootstrapping": bootstrapping_curve,
            "nelson_siegel": nelson_siegel_curve,
            "svensson": svensson_curve,
            "kernel_ridge": kernel_ridge_curve,
        },
        ltn_observations=curve_inputs,
        swap_observations=swap_observations,
        calendar=calendar,
        parameters=parameters,
    )
    return tuple(results[key] for key in OUTPUT_KEYS)
