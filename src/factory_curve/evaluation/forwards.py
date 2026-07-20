from __future__ import annotations

import numpy as np
import pandas as pd

from .contracts import EvaluationContext


class ForwardArbitrageCalculator:
    """Diagnose invalid or economically bounded one-BU implied forwards."""

    result_keys = (
        "forward_diagnostics_daily",
        "forward_violations",
    )

    def calculate(
        self,
        context: EvaluationContext,
    ) -> dict[str, pd.DataFrame]:
        rates = context.curve.values.to_numpy(copy=False)
        tenors = context.curve.tenors.to_numpy(dtype=np.float64)
        bd_year = int(
            context.parameters.get("business_days_per_year", 252)
        )
        minimum = float(
            context.parameters.get("minimum_forward_rate", 0.0)
        )
        maximum = float(
            context.parameters.get("maximum_forward_rate", 1.0)
        )
        if minimum <= -1.0 or maximum <= minimum:
            raise ValueError(
                "Forward bounds must satisfy -1 < minimum < maximum"
            )

        valid_rates = np.isfinite(rates) & (rates > -1.0)
        safe_rates = np.where(valid_rates, rates, 0.0)
        log_discount = np.where(
            valid_rates,
            -(tenors[None, :] / bd_year) * np.log1p(safe_rates),
            np.nan,
        )
        previous = np.column_stack(
            [np.zeros(len(log_discount)), log_discount[:, :-1]]
        )
        tenor_steps = np.diff(np.concatenate(([0.0], tenors)))
        annualized_log_forward = (
            -(log_discount - previous)
            * bd_year
            / tenor_steps[None, :]
        )
        finite_log_forward = np.isfinite(annualized_log_forward)
        maximum_representable_log = np.log(np.finfo(np.float64).max)
        representable = (
            finite_log_forward
            & (annualized_log_forward <= maximum_representable_log)
        )
        forward = np.full_like(annualized_log_forward, np.nan)
        forward[representable] = np.expm1(
            annualized_log_forward[representable]
        )

        invalid = ~finite_log_forward
        below = (
            finite_log_forward
            & (annualized_log_forward < np.log1p(minimum))
        )
        above = (
            finite_log_forward
            & (annualized_log_forward > np.log1p(maximum))
        )
        violation = invalid | below | above

        finite_forward = np.isfinite(forward)
        forward_count = finite_forward.sum(axis=1)
        forward_sum = np.where(finite_forward, forward, 0.0).sum(axis=1)
        minimum_forward = np.where(
            finite_forward, forward, np.inf
        ).min(axis=1)
        maximum_forward = np.where(
            finite_forward, forward, -np.inf
        ).max(axis=1)
        minimum_forward[forward_count == 0] = np.nan
        maximum_forward[forward_count == 0] = np.nan
        daily = pd.DataFrame(
            {
                "methodology": context.methodology,
                "ref_date": context.curve.dates,
                "n_forwards": finite_log_forward.sum(axis=1),
                "n_violations": violation.sum(axis=1),
                "minimum_forward_rate": minimum_forward,
                "maximum_forward_rate": maximum_forward,
                "mean_forward_rate": np.divide(
                    forward_sum,
                    forward_count,
                    out=np.full(len(forward_count), np.nan),
                    where=forward_count > 0,
                ),
                "maximum_annualized_log_forward": np.where(
                    finite_log_forward,
                    annualized_log_forward,
                    -np.inf,
                ).max(axis=1),
            }
        )
        daily.loc[
            ~np.isfinite(daily["maximum_annualized_log_forward"]),
            "maximum_annualized_log_forward",
        ] = np.nan
        daily["violation_share"] = (
            daily["n_violations"] / len(context.curve.tenors)
        )

        date_positions, tenor_positions = np.where(violation)
        reasons = np.where(
            invalid[date_positions, tenor_positions],
            "invalid_forward",
            np.where(
                below[date_positions, tenor_positions],
                "below_minimum",
                "above_maximum",
            ),
        )
        violations = pd.DataFrame(
            {
                "methodology": context.methodology,
                "ref_date": context.curve.dates[date_positions],
                "tenor_bd": context.curve.tenors.to_numpy()[tenor_positions],
                "implied_forward_rate": forward[
                    date_positions, tenor_positions
                ],
                "annualized_log_forward": annualized_log_forward[
                    date_positions, tenor_positions
                ],
                "violation_reason": reasons,
            }
        )
        return {
            "forward_diagnostics_daily": daily,
            "forward_violations": violations,
        }
