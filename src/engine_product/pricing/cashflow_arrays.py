from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class CashflowScheduleArrays:
    """
    Compact pricing view of an instrument cashflow schedule.

    The source cashflow dimension can keep separate cashflow rows by type.
    For yield pricing, amounts on the same payment business-day index are
    aggregated because they share the same discount factor.
    """

    payment_bd_index: np.ndarray
    amount: np.ndarray

    def future_arrays_as_of(
        self,
        ref_bd_index: int,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Return future tenors in business days and their aggregated amounts."""

        mask = self.payment_bd_index > ref_bd_index
        return (
            self.payment_bd_index[mask] - int(ref_bd_index),
            self.amount[mask],
        )

    def time_amount_pairs_as_of(
        self,
        ref_bd_index: int,
    ) -> tuple[tuple[float, float], ...]:
        tenor_bd, amounts = self.future_arrays_as_of(ref_bd_index)

        if tenor_bd.size == 0:
            return tuple()

        times = tenor_bd / 252.0

        return tuple(
            (float(t), float(amount))
            for t, amount in zip(times, amounts, strict=True)
            if t > 0.0
        )


def as_date(value: Any) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    return pd.to_datetime(value).date()


def build_bd_index_lookup(
    calendar_df: pd.DataFrame,
) -> dict[date, int]:
    calendar_df = calendar_df.copy()
    calendar_df["date"] = pd.to_datetime(calendar_df["date"]).dt.date

    return {
        as_date(row.date): int(row.bd_index)
        for row in calendar_df.itertuples(index=False)
    }


def build_cashflow_schedule_lookup(
    cashflow_dimension: pd.DataFrame,
) -> dict[str, CashflowScheduleArrays]:
    """
    Build a compact per-ISIN lookup from the granular cashflow dimension.
    """
    required_columns = {"isin", "payment_bd_index", "amount"}
    missing_columns = required_columns - set(cashflow_dimension.columns)

    if missing_columns:
        raise ValueError(
            "cashflow_dimension is missing required columns: "
            f"{sorted(missing_columns)}"
        )

    if cashflow_dimension.empty:
        return {}

    pricing_cashflows = (
        cashflow_dimension
        .groupby(["isin", "payment_bd_index"], as_index=False)
        .agg(amount=("amount", "sum"))
        .sort_values(["isin", "payment_bd_index"])
    )

    lookup: dict[str, CashflowScheduleArrays] = {}

    for isin, group in pricing_cashflows.groupby("isin", sort=False):
        lookup[str(isin)] = CashflowScheduleArrays(
            payment_bd_index=group["payment_bd_index"].to_numpy(dtype=np.int64),
            amount=group["amount"].to_numpy(dtype=float),
        )

    return lookup


def price_from_time_amount_pairs(
    time_amount_pairs: Iterable[tuple[float, float]],
    ytm: float,
) -> float:
    if ytm <= -1.0:
        raise ValueError("ytm must be greater than -1")

    pairs = tuple(time_amount_pairs)

    if not pairs:
        raise ValueError("time_amount_pairs must contain at least one future cashflow")

    times = np.asarray([t for t, _ in pairs], dtype=float)
    amounts = np.asarray([amount for _, amount in pairs], dtype=float)

    return float(np.sum(amounts / np.power(1.0 + ytm, times)))


def macaulay_duration_from_time_amount_pairs(
    time_amount_pairs: Iterable[tuple[float, float]],
    ytm: float,
) -> float:
    if ytm <= -1.0:
        raise ValueError("ytm must be greater than -1")

    pairs = tuple(time_amount_pairs)

    if not pairs:
        raise ValueError("time_amount_pairs must contain at least one future cashflow")

    times = np.asarray([t for t, _ in pairs], dtype=float)
    amounts = np.asarray([amount for _, amount in pairs], dtype=float)
    present_values = amounts / np.power(1.0 + ytm, times)
    price = float(np.sum(present_values))

    if price <= 0.0:
        raise ValueError("price must be positive to compute duration")

    return float(np.sum(times * present_values) / price)
