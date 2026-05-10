from dataclasses import dataclass, field
from datetime import date
from typing import Iterable

from engine_product.cashflows.models import Cashflow
from engine_product.convention import DayCountConventionRepository


@dataclass(frozen=True)
class YieldProblem:
    cashflows: list[Cashflow]
    market_price: float
    settlement_date: date
    day_count: DayCountConventionRepository
    _future_cashflows: tuple[Cashflow, ...] = field(init=False, repr=False)
    _time_amount_pairs: tuple[tuple[float, float], ...] = field(init=False, repr=False)

    def __post_init__(self):
        if self.market_price <= 0:
            raise ValueError("market_price must be positive")

        future_cashflows = tuple(
            cf for cf in self.cashflows
            if cf.payment_date > self.settlement_date
        )

        if not future_cashflows:
            raise ValueError("cashflows must contain at least one future cashflow")

        time_amount_pairs = tuple(
            (t, cf.amount)
            for cf in future_cashflows
            if (t := self.day_count.year_fraction(
                self.settlement_date,
                cf.payment_date,
            )) > 0
        )

        if not time_amount_pairs:
            raise ValueError("cashflows must contain at least one valid future cashflow")

        object.__setattr__(self, "_future_cashflows", future_cashflows)
        object.__setattr__(self, "_time_amount_pairs", time_amount_pairs)

    @classmethod
    def from_time_amount_pairs(
        cls,
        *,
        time_amount_pairs: Iterable[tuple[float, float]],
        market_price: float,
    ) -> "YieldProblem":
        """
        Build a YieldProblem directly from precomputed (time, amount) pairs.

        This is useful for batch pipelines that already computed day-counts,
        schedules, or grouped cashflow structures outside the unit object.
        """
        pairs = tuple(
            (float(t), float(amount))
            for t, amount in time_amount_pairs
            if float(t) > 0
        )

        if market_price <= 0:
            raise ValueError("market_price must be positive")

        if not pairs:
            raise ValueError("time_amount_pairs must contain at least one valid future cashflow")

        problem = object.__new__(cls)

        object.__setattr__(problem, "cashflows", [])
        object.__setattr__(problem, "market_price", float(market_price))
        object.__setattr__(problem, "settlement_date", None)
        object.__setattr__(problem, "day_count", None)
        object.__setattr__(problem, "_future_cashflows", tuple())
        object.__setattr__(problem, "_time_amount_pairs", pairs)

        return problem

    @property
    def future_cashflows(self) -> tuple[Cashflow, ...]:
        return self._future_cashflows

    @property
    def time_amount_pairs(self) -> tuple[tuple[float, float], ...]:
        return self._time_amount_pairs

    @property
    def is_single_cashflow(self) -> bool:
        return len(self._time_amount_pairs) == 1

    def zero_coupon_yield(self) -> float:
        """
        Closed-form YTM for any problem with exactly one future cashflow.

        This is instrument-agnostic: it works for LTN, but also for any other
        single-payment instrument.
        """
        if not self.is_single_cashflow:
            raise ValueError("zero_coupon_yield requires exactly one future cashflow")

        t, amount = self._time_amount_pairs[0]

        if t <= 0:
            raise ValueError("single cashflow time must be positive")

        if amount <= 0:
            raise ValueError("single cashflow amount must be positive")

        return (amount / self.market_price) ** (1.0 / t) - 1.0

    def price_from_yield(self, ytm: float) -> float:
        if ytm <= -1.0:
            raise ValueError("ytm must be greater than -1")

        return sum(
            amount / ((1.0 + ytm) ** t)
            for t, amount in self._time_amount_pairs
        )

    def objective(self, ytm: float) -> float:
        return self.price_from_yield(ytm) - self.market_price

    def derivative(self, ytm: float) -> float:
        if ytm <= -1.0:
            raise ValueError("ytm must be greater than -1")

        return sum(
            -t * amount / ((1.0 + ytm) ** (t + 1.0))
            for t, amount in self._time_amount_pairs
        )
