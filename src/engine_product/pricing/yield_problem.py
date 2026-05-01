# engine_product/pricing/yield_problem.py

from dataclasses import dataclass
from datetime import date

from engine_product.cashflows.models import Cashflow
from engine_product.convention import DayCountConventionRepository


@dataclass(frozen=True)
class YieldProblem:
    cashflows: list[Cashflow]
    market_price: float
    settlement_date: date
    day_count: DayCountConventionRepository

    def __post_init__(self):
        if self.market_price <= 0:
            raise ValueError("market_price must be positive")

        if not self.future_cashflows:
            raise ValueError("cashflows must contain at least one future cashflow")

        if not self.time_amount_pairs:
            raise ValueError("cashflows must contain at least one valid future cashflow")

    @property
    def future_cashflows(self) -> list[Cashflow]:
        return [
            cf for cf in self.cashflows
            if cf.payment_date > self.settlement_date
        ]

    @property
    def time_amount_pairs(self) -> list[tuple[float, float]]:
        pairs = []

        for cf in self.future_cashflows:
            t = self.day_count.year_fraction(
                self.settlement_date,
                cf.payment_date,
            )

            if t > 0:
                pairs.append((t, cf.amount))

        return pairs

    def price_from_yield(self, ytm: float) -> float:
        if ytm <= -1.0:
            raise ValueError("ytm must be greater than -1")

        return sum(
            amount / ((1.0 + ytm) ** t)
            for t, amount in self.time_amount_pairs
        )

    def objective(self, ytm: float) -> float:
        return self.price_from_yield(ytm) - self.market_price

    def derivative(self, ytm: float) -> float:
        if ytm <= -1.0:
            raise ValueError("ytm must be greater than -1")

        return sum(
            -t * amount / ((1.0 + ytm) ** (t + 1.0))
            for t, amount in self.time_amount_pairs
        )