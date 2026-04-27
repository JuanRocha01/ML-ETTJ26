from datetime import date

from engine_product.convention import DayCountConventionRepository


class FixedRateIndexer:
    def __init__(
        self,
        annual_rate: float,
        day_count: DayCountConventionRepository,
    ):
        self.annual_rate = annual_rate
        self.day_count = day_count

    def accrual_factor(self, start: date, end: date) -> float:
        yf = self.day_count.year_fraction(start, end)
        return self.annual_rate * yf