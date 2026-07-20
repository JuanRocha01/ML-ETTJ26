from __future__ import annotations

import numpy as np
import pandas as pd

from .contracts import EvaluationContext
from .rate_fit import build_rate_points


def zero_coupon_price(
    rate: np.ndarray | pd.Series | float,
    tenor_bd: np.ndarray | pd.Series | float,
    *,
    notional: float,
    business_days_per_year: int,
) -> np.ndarray:
    rates, tenors = np.broadcast_arrays(
        np.asarray(rate, dtype=np.float64),
        np.asarray(tenor_bd, dtype=np.float64),
    )
    result = np.full(rates.shape, np.nan, dtype=np.float64)
    if (
        not np.isfinite(notional)
        or notional <= 0.0
        or business_days_per_year <= 0
    ):
        return result

    valid = (
        np.isfinite(rates)
        & np.isfinite(tenors)
        & (rates > -1.0)
        & (tenors >= 0.0)
    )
    if not valid.any():
        return result

    log_prices = (
        np.log(notional)
        - (tenors[valid] / business_days_per_year)
        * np.log1p(rates[valid])
    )
    representable = (
        np.isfinite(log_prices)
        & (log_prices <= np.log(np.finfo(np.float64).max))
    )
    valid_positions = np.flatnonzero(valid)
    result.flat[valid_positions[representable]] = np.exp(
        log_prices[representable]
    )
    return result


class RepricingCalculator:
    """Reprice LTN observations and zero-coupon equivalents of swap rates."""

    result_keys = (
        "repricing_errors",
        "repricing_metrics_daily",
        "repricing_metrics_summary",
    )

    def calculate(
        self,
        context: EvaluationContext,
    ) -> dict[str, pd.DataFrame]:
        rate_points = build_rate_points(context)
        config = context.parameters
        notional = float(config.get("notional", 1000.0))
        bd_year = int(config.get("business_days_per_year", 252))

        model_price = zero_coupon_price(
            rate_points["estimated_rate"],
            rate_points["tenor_bd"],
            notional=notional,
            business_days_per_year=bd_year,
        )
        theoretical_market_price = zero_coupon_price(
            rate_points["observed_rate"],
            rate_points["tenor_bd"],
            notional=notional,
            business_days_per_year=bd_year,
        )
        rate_points["observed_price"] = np.where(
            rate_points["sample"].eq("in_sample"),
            rate_points["observed_price"],
            theoretical_market_price,
        )
        rate_points["estimated_price"] = model_price
        valid_estimated_rate = (
            np.isfinite(rate_points["estimated_rate"])
            & rate_points["estimated_rate"].gt(-1.0)
        )
        valid_observed_price = (
            np.isfinite(rate_points["observed_price"])
            & rate_points["observed_price"].gt(0.0)
        )
        valid_estimated_price = (
            np.isfinite(rate_points["estimated_price"])
            & rate_points["estimated_price"].gt(0.0)
        )
        rate_points["pricing_status"] = np.select(
            [
                ~valid_estimated_rate,
                ~valid_observed_price,
                ~valid_estimated_price,
            ],
            [
                "invalid_estimated_rate",
                "invalid_observed_price",
                "non_representable_estimated_price",
            ],
            default="valid",
        )
        rate_points["price_error"] = (
            rate_points["estimated_price"] - rate_points["observed_price"]
        )
        rate_points["relative_price_error"] = (
            rate_points["price_error"] / rate_points["observed_price"]
        )
        errors = rate_points[
            [
                "methodology",
                "sample",
                "ref_date",
                "instrument_id",
                "tenor_bd",
                "observed_price",
                "estimated_price",
                "price_error",
                "relative_price_error",
                "pricing_status",
            ]
        ].reset_index(drop=True)

        daily_metrics = _repricing_metrics(errors, daily=True)
        summary_metrics = _repricing_metrics(errors, daily=False)
        return {
            "repricing_errors": errors,
            "repricing_metrics_daily": daily_metrics,
            "repricing_metrics_summary": summary_metrics,
        }


def _repricing_metrics(
    errors: pd.DataFrame,
    *,
    daily: bool,
) -> pd.DataFrame:
    grouping = ["methodology", "sample"]
    if daily:
        grouping.append("ref_date")
    valid = errors.loc[np.isfinite(errors["price_error"])].copy()
    valid["abs_error"] = valid["price_error"].abs()
    valid["squared_error"] = valid["price_error"].pow(2)
    valid["abs_relative_error"] = valid["relative_price_error"].abs()
    metrics = (
        valid.groupby(grouping, as_index=False, observed=True)
        .agg(
            n_observations=("price_error", "size"),
            mean_squared_error=("squared_error", "mean"),
            mae=("abs_error", "mean"),
            bias=("price_error", "mean"),
            mape=("abs_relative_error", "mean"),
            max_abs_error=("abs_error", "max"),
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
            "mape",
            "max_abs_error",
        ]
    ].reset_index(drop=True)
