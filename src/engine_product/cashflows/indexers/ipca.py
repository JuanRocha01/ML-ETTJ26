from datetime import date


class IPCAPlusSpreadIndexer:
    def __init__(
        self,
        inflation_curve,
        spread: float,
        day_count,
    ):
        self.inflation_curve = inflation_curve
        self.spread = spread
        self.day_count = day_count

    def accrual_factor(self, start: date, end: date) -> float:
        inflation_factor = self.inflation_curve.accumulated_factor(start, end)
        spread_factor = self.spread * self.day_count.year_fraction(start, end)

        return (inflation_factor - 1.0) + spread_factor