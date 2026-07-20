from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .contracts import EvaluationContext
from .repricing import zero_coupon_price


@dataclass(frozen=True)
class RolldownTarget:
    segment: str
    target_bd: int


def _repeated_ltn_pairs(
    observations: pd.DataFrame,
    calendar: pd.DataFrame,
) -> pd.DataFrame:
    ltn = observations.loc[
        observations["instrument_type"].eq("LTN")
    ].copy()
    ltn["ref_date"] = pd.to_datetime(ltn["ref_date"]).dt.normalize()
    business_calendar = calendar.loc[
        calendar["is_business_day"], ["date", "bd_index"]
    ].copy()
    business_calendar["date"] = pd.to_datetime(
        business_calendar["date"]
    ).dt.normalize()
    ltn = ltn.merge(
        business_calendar.rename(
            columns={"date": "ref_date", "bd_index": "ref_bd_index"}
        ),
        on="ref_date",
        how="inner",
    )
    next_rows = ltn[
        ["isin", "ref_bd_index", "ref_date", "market_ytm", "market_pu"]
    ].rename(
        columns={
            "ref_bd_index": "next_bd_index",
            "ref_date": "next_ref_date",
            "market_ytm": "next_market_ytm",
            "market_pu": "next_market_pu",
        }
    )
    ltn["next_bd_index"] = ltn["ref_bd_index"] + 1
    return ltn.merge(
        next_rows,
        on=["isin", "next_bd_index"],
        how="inner",
        validate="many_to_one",
    )


def _targets(parameters: dict) -> list[RolldownTarget]:
    result: list[RolldownTarget] = []
    for segment in ("short", "medium", "long"):
        for tenor in parameters.get(
            f"rolldown_{segment}_targets_bd", ()
        ):
            result.append(RolldownTarget(segment, int(tenor)))
    if len(result) != 4:
        raise ValueError(
            "Rolldown configuration must define exactly one short, "
            "two medium and one long target"
        )
    return result


def _segment_mask(
    frame: pd.DataFrame,
    target: RolldownTarget,
    short_end: int,
    medium_end: int,
) -> pd.Series:
    tenor = frame["bd_to_maturity"]
    if target.segment == "short":
        return tenor.le(short_end)
    if target.segment == "medium":
        return tenor.gt(short_end) & tenor.le(medium_end)
    return tenor.gt(medium_end)


def _select_for_date(
    frame: pd.DataFrame,
    targets: list[RolldownTarget],
    *,
    short_end: int,
    medium_end: int,
) -> pd.DataFrame:
    selected: list[pd.Series] = []
    used_isins: set[str] = set()
    for target in targets:
        eligible = frame.loc[
            _segment_mask(frame, target, short_end, medium_end)
            & ~frame["isin"].astype(str).isin(used_isins)
        ].copy()
        if eligible.empty:
            continue
        eligible["target_distance"] = (
            eligible["bd_to_maturity"] - target.target_bd
        ).abs()
        row = eligible.sort_values(
            ["target_distance", "bd_to_maturity", "isin"]
        ).iloc[0].copy()
        row["tenor_segment"] = target.segment
        row["target_bd"] = target.target_bd
        selected.append(row)
        used_isins.add(str(row["isin"]))
    return pd.DataFrame(selected)


def _monthly_sample(
    pairs: pd.DataFrame,
    curve_dates: pd.DatetimeIndex,
    parameters: dict,
) -> pd.DataFrame:
    start = pd.Timestamp(parameters.get("rolldown_start_date", "2020-01-01"))
    end = pd.Timestamp(parameters.get("rolldown_end_date", "2026-12-31"))
    eligible = pairs.loc[
        pairs["ref_date"].between(start, end)
        & pairs["ref_date"].isin(curve_dates)
    ].copy()
    eligible["month"] = eligible["ref_date"].dt.to_period("M")
    targets = _targets(parameters)
    short_end = int(parameters.get("rolldown_short_end_bd", 504))
    medium_end = int(parameters.get("rolldown_medium_end_bd", 1260))

    monthly: list[pd.DataFrame] = []
    for _, month in eligible.groupby("month", sort=True):
        candidates = [
            _select_for_date(
                day,
                targets,
                short_end=short_end,
                medium_end=medium_end,
            )
            for _, day in month.groupby("ref_date", sort=True)
        ]
        candidates = [candidate for candidate in candidates if not candidate.empty]
        if candidates:
            best_size = max(len(candidate) for candidate in candidates)
            monthly.append(
                next(
                    candidate
                    for candidate in candidates
                    if len(candidate) == best_size
                )
            )
    if not monthly:
        return pd.DataFrame()
    return pd.concat(monthly, ignore_index=True)


class RolldownCalculator:
    """One-business-day LTN prediction with central-difference Taylor terms."""

    result_keys = ("rolldown_results", "rolldown_metrics")

    def calculate(
        self,
        context: EvaluationContext,
    ) -> dict[str, pd.DataFrame]:
        parameters = context.parameters
        pairs = _repeated_ltn_pairs(
            context.ltn_observations,
            context.calendar,
        )
        sample = _monthly_sample(
            pairs,
            context.curve.dates,
            parameters,
        )
        if sample.empty:
            return {
                "rolldown_results": sample,
                "rolldown_metrics": pd.DataFrame(),
            }

        tenor = sample["bd_to_maturity"].astype(int).to_numpy()
        rolled_tenor = tenor - 1
        derivative_step = int(
            parameters.get("curve_derivative_step_bd", 5)
        )
        ref_date = sample["ref_date"]
        rate_at_t = context.curve.lookup(ref_date, tenor)
        rate_rolled_direct = context.curve.lookup(ref_date, rolled_tenor)
        rate_plus = context.curve.lookup(ref_date, tenor + derivative_step)
        rate_minus = context.curve.lookup(ref_date, tenor - derivative_step)
        first_derivative = (
            (rate_plus - rate_minus) / (2.0 * derivative_step)
        )
        second_derivative = (
            (rate_plus - 2.0 * rate_at_t + rate_minus)
            / derivative_step**2
        )
        rate_rolled_taylor = (
            rate_at_t - first_derivative + 0.5 * second_derivative
        )

        notional = float(parameters.get("notional", 1000.0))
        bd_year = int(parameters.get("business_days_per_year", 252))
        base_rolled_price = zero_coupon_price(
            rate_at_t,
            rolled_tenor,
            notional=notional,
            business_days_per_year=bd_year,
        )
        bump = float(parameters.get("price_sensitivity_bump", 0.0001))
        price_plus = zero_coupon_price(
            rate_at_t + bump,
            rolled_tenor,
            notional=notional,
            business_days_per_year=bd_year,
        )
        price_minus = zero_coupon_price(
            rate_at_t - bump,
            rolled_tenor,
            notional=notional,
            business_days_per_year=bd_year,
        )
        price_first_derivative = (
            (price_plus - price_minus) / (2.0 * bump)
        )
        price_second_derivative = (
            (price_plus - 2.0 * base_rolled_price + price_minus)
            / bump**2
        )
        delta_rate = rate_rolled_taylor - rate_at_t
        price_rolled_taylor = (
            base_rolled_price
            + price_first_derivative * delta_rate
            + 0.5 * price_second_derivative * delta_rate**2
        )
        price_rolled_direct = zero_coupon_price(
            rate_rolled_direct,
            rolled_tenor,
            notional=notional,
            business_days_per_year=bd_year,
        )

        result = pd.DataFrame(
            {
                "methodology": context.methodology,
                "ref_date": sample["ref_date"],
                "next_ref_date": sample["next_ref_date"],
                "isin": sample["isin"].astype(str),
                "tenor_segment": sample["tenor_segment"],
                "target_bd": sample["target_bd"].astype(int),
                "tenor_bd": tenor,
                "rolled_tenor_bd": rolled_tenor,
                "market_rate_d": sample["market_ytm"].astype(float),
                "actual_rate_d1": sample["next_market_ytm"].astype(float),
                "curve_rate_d": rate_at_t,
                "predicted_rate_d1_direct": rate_rolled_direct,
                "predicted_rate_d1_taylor": rate_rolled_taylor,
                "curve_first_derivative": first_derivative,
                "curve_second_derivative": second_derivative,
                "actual_price_d1": sample["next_market_pu"].astype(float),
                "predicted_price_d1_direct": price_rolled_direct,
                "predicted_price_d1_taylor": price_rolled_taylor,
                "price_first_derivative": price_first_derivative,
                "price_second_derivative": price_second_derivative,
            }
        )
        result["rate_error_direct"] = (
            result["predicted_rate_d1_direct"] - result["actual_rate_d1"]
        )
        result["rate_error_taylor"] = (
            result["predicted_rate_d1_taylor"] - result["actual_rate_d1"]
        )
        result["price_error_direct"] = (
            result["predicted_price_d1_direct"] - result["actual_price_d1"]
        )
        result["price_error_taylor"] = (
            result["predicted_price_d1_taylor"] - result["actual_price_d1"]
        )
        result = result.replace([np.inf, -np.inf], np.nan).dropna(
            subset=[
                "predicted_rate_d1_direct",
                "predicted_rate_d1_taylor",
                "actual_rate_d1",
                "predicted_price_d1_direct",
                "predicted_price_d1_taylor",
                "actual_price_d1",
                "price_first_derivative",
                "price_second_derivative",
            ]
        )
        result = result.sort_values(
                ["methodology", "ref_date", "target_bd"]
            ).reset_index(drop=True)
        metric_rows: list[dict] = []
        for segment, group in result.groupby(
            "tenor_segment", observed=True, sort=True
        ):
            for prediction_method in ("direct", "taylor"):
                rate_error = group[f"rate_error_{prediction_method}"]
                price_error = group[f"price_error_{prediction_method}"]
                metric_rows.append(
                    {
                        "methodology": context.methodology,
                        "tenor_segment": segment,
                        "prediction_method": prediction_method,
                        "n_observations": len(group),
                        "rate_rmse": float(
                            np.sqrt(np.mean(np.square(rate_error)))
                        ),
                        "rate_mae": float(np.mean(np.abs(rate_error))),
                        "rate_bias": float(np.mean(rate_error)),
                        "price_rmse": float(
                            np.sqrt(np.mean(np.square(price_error)))
                        ),
                        "price_mae": float(np.mean(np.abs(price_error))),
                        "price_bias": float(np.mean(price_error)),
                    }
                )
        return {
            "rolldown_results": result,
            "rolldown_metrics": pd.DataFrame(metric_rows),
        }
