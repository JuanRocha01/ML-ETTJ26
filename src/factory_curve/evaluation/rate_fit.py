from __future__ import annotations

import numpy as np
import pandas as pd

from .contracts import EvaluationContext


def _rate_metrics(
    errors: pd.DataFrame,
    *,
    daily: bool,
) -> pd.DataFrame:
    grouping = ["methodology", "sample"]
    if daily:
        grouping.append("ref_date")
    valid = errors.loc[np.isfinite(errors["rate_error"])].copy()
    if valid.empty:
        return pd.DataFrame(
            columns=[
                *grouping,
                "n_observations",
                "rmse",
                "mae",
                "bias",
                "max_abs_error",
            ]
        )
    valid["absolute_error"] = valid["rate_error"].abs()
    valid["squared_error"] = valid["rate_error"].pow(2)
    metrics = (
        valid.groupby(
            grouping,
            as_index=False,
            observed=True,
        )
        .agg(
            n_observations=("rate_error", "size"),
            mean_squared_error=("squared_error", "mean"),
            mae=("absolute_error", "mean"),
            bias=("rate_error", "mean"),
            max_abs_error=("absolute_error", "max"),
        )
        .sort_values(grouping)
    )
    metrics["rmse"] = np.sqrt(metrics.pop("mean_squared_error"))
    return metrics[
        [
            *grouping,
            "n_observations",
            "rmse",
            "mae",
            "bias",
            "max_abs_error",
        ]
    ].reset_index(drop=True)


def build_rate_points(context: EvaluationContext) -> pd.DataFrame:
    """Prepare comparable market/curve points for both evaluation samples."""

    ltn = context.ltn_observations.loc[
        context.ltn_observations["instrument_type"].eq("LTN")
    ].copy()
    ltn["ref_date"] = pd.to_datetime(ltn["ref_date"]).dt.normalize()
    ltn_points = pd.DataFrame(
        {
            "methodology": context.methodology,
            "sample": "in_sample",
            "ref_date": ltn["ref_date"],
            "instrument_id": ltn["isin"].astype(str),
            "tenor_bd": pd.to_numeric(
                ltn["bd_to_maturity"], errors="coerce"
            ),
            "observed_rate": pd.to_numeric(
                ltn["market_ytm"], errors="coerce"
            ),
            "observed_price": pd.to_numeric(
                ltn["market_pu"], errors="coerce"
            ),
        }
    )

    swaps = context.swap_observations.copy()
    scale = float(context.parameters.get("swap_rate_scale", 0.01))
    swap_dates = pd.to_datetime(swaps["date"]).dt.normalize()
    maturity = pd.to_datetime(swaps["maturity"]).dt.normalize()
    swap_points = pd.DataFrame(
        {
            "methodology": context.methodology,
            "sample": "out_of_sample",
            "ref_date": swap_dates,
            "instrument_id": (
                swaps["product_code"].astype(str)
                + ":"
                + maturity.dt.strftime("%Y-%m-%d")
            ),
            "tenor_bd": pd.to_numeric(
                swaps["bd_to_maturity"], errors="coerce"
            ),
            "observed_rate": (
                pd.to_numeric(swaps["adjusted_value"], errors="coerce")
                * scale
            ),
            "observed_price": np.nan,
        }
    )
    points = pd.concat([ltn_points, swap_points], ignore_index=True)
    points["estimated_rate"] = context.curve.lookup(
        points["ref_date"],
        points["tenor_bd"],
    )
    points["rate_error"] = (
        points["estimated_rate"] - points["observed_rate"]
    )
    points = points.loc[
        points["tenor_bd"].gt(0)
        & points["observed_rate"].gt(-1.0)
        & np.isfinite(points["estimated_rate"])
    ].sort_values(
        ["methodology", "sample", "ref_date", "tenor_bd", "instrument_id"]
    )
    points["tenor_bd"] = points["tenor_bd"].astype("int32")
    return points.reset_index(drop=True)


class RateFitCalculator:
    """In-sample LTN and out-of-sample DI x PRE rate errors."""

    result_keys = (
        "rate_errors",
        "rate_metrics_daily",
        "rate_metrics_summary",
    )

    def calculate(
        self,
        context: EvaluationContext,
    ) -> dict[str, pd.DataFrame]:
        points = build_rate_points(context)
        return {
            "rate_errors": points,
            "rate_metrics_daily": _rate_metrics(points, daily=True),
            "rate_metrics_summary": _rate_metrics(points, daily=False),
        }
