from datetime import date

from engine_product.convention import DayCountConventionRepository


class CompoundFixedRateIndexer:
    def __init__(
        self,
        annual_rate: float,
        day_count: DayCountConventionRepository,
    ):
        self.annual_rate = annual_rate
        self.day_count = day_count

    def accrual_factor(self, start: date, end: date) -> float:
        yf = self.day_count.year_fraction(start, end)
        return (1 + self.annual_rate) ** yf - 1


class PeriodicFixedCouponIndexer:
    def __init__(
        self,
        annual_rate: float,
        frequency: int,
    ):
        if annual_rate <= -1.0:
            raise ValueError("annual_rate must be greater than -1")

        if frequency <= 0:
            raise ValueError("frequency must be positive")

        self.annual_rate = annual_rate
        self.frequency = frequency

    def accrual_factor(self, start, end) -> float:
        return (1.0 + self.annual_rate) ** (1.0 / self.frequency) - 1.0