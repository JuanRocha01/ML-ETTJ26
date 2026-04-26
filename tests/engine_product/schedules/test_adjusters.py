from datetime import date, timedelta

from engine_product.schedules.adjusters import (
    following,
    nearest_business_day,
    preceding,
    unadjusted,
)


class FakeCalendar:
    def __init__(self, business_days):
        self.business_days = set(business_days)

    def is_business_day(self, d: date) -> bool:
        return d in self.business_days

    def adjust_next_business_day(self, d: date) -> date:
        current = d

        while current not in self.business_days:
            current += timedelta(days=1)

        return current

    def adjust_previous_business_day(self, d: date) -> date:
        current = d

        while current not in self.business_days:
            current -= timedelta(days=1)

        return current


def test_unadjusted_returns_same_date():
    adjuster = unadjusted()

    result = adjuster(date(2026, 1, 3))

    assert result == date(2026, 1, 3)


def test_following_adjusts_to_next_business_day():
    calendar = FakeCalendar(
        business_days=[
            date(2026, 1, 5),
        ]
    )

    adjuster = following(calendar)

    result = adjuster(date(2026, 1, 3))

    assert result == date(2026, 1, 5)


def test_preceding_adjusts_to_previous_business_day():
    calendar = FakeCalendar(
        business_days=[
            date(2026, 1, 2),
        ]
    )

    adjuster = preceding(calendar)

    result = adjuster(date(2026, 1, 3))

    assert result == date(2026, 1, 2)


def test_nearest_business_day_returns_same_date_if_business_day():
    calendar = FakeCalendar(
        business_days=[
            date(2026, 1, 5),
        ]
    )

    adjuster = nearest_business_day(calendar)

    result = adjuster(date(2026, 1, 5))

    assert result == date(2026, 1, 5)


def test_nearest_business_day_returns_closest_next_business_day():
    calendar = FakeCalendar(
        business_days=[
            date(2026, 1, 2),
            date(2026, 1, 5),
        ]
    )

    adjuster = nearest_business_day(calendar)

    result = adjuster(date(2026, 1, 4))

    assert result == date(2026, 1, 5)


def test_nearest_business_day_prefers_previous_when_tie():
    calendar = FakeCalendar(
        business_days=[
            date(2026, 1, 2),
            date(2026, 1, 4),
        ]
    )

    adjuster = nearest_business_day(calendar)

    result = adjuster(date(2026, 1, 3))

    assert result == date(2026, 1, 2)