from datetime import date


class CDIPlusSpreadIndexer:
    def __init__(
        self,
        cdi_curve,
        spread: float,
        day_count,
    ):
        self.cdi_curve = cdi_curve
        self.spread = spread
        self.day_count = day_count

    def accrual_factor(self, start: date, end: date) -> float:
        cdi_factor = self.cdi_curve.accumulated_factor(start, end)
        spread_factor = self.spread * self.day_count.year_fraction(start, end)

        return (cdi_factor - 1.0) + spread_factor