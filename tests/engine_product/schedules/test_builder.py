from datetime import date, timedelta

from engine_product.schedules.builder import ScheduleBuilder
from engine_product.schedules.rules import (
    custom_dates,
    every_n_months_backward,
    first_day_of_months,
)
from engine_product.schedules.adjusters import following, unadjusted


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


def test_builder_builds_schedule_from_custom_dates():
    schedule = (
        ScheduleBuilder()
        .seed(
            rule=custom_dates(
                [
                    date(2026, 1, 10),
                    date(2026, 7, 10),
                    date(2027, 1, 10),
                ]
            ),
            start=date(2026, 1, 1),
            maturity=date(2026, 12, 31),
        )
        .adjust(unadjusted())
        .normalize()
        .build()
    )

    assert schedule == [
        date(2026, 1, 10),
        date(2026, 7, 10),
    ]


def test_builder_adds_boundaries_and_normalizes():
    schedule = (
        ScheduleBuilder()
        .seed(
            rule=custom_dates(
                [
                    date(2026, 7, 10),
                    date(2026, 7, 10),
                ]
            ),
            start=date(2026, 1, 10),
            maturity=date(2027, 1, 10),
        )
        .add_dates(date(2026, 1, 10), date(2027, 1, 10))
        .normalize()
        .build()
    )

    assert schedule == [
        date(2026, 1, 10),
        date(2026, 7, 10),
        date(2027, 1, 10),
    ]


def test_builder_filters_between_dates():
    schedule = (
        ScheduleBuilder()
        .seed(
            rule=custom_dates(
                [
                    date(2026, 1, 1),
                    date(2026, 7, 1),
                    date(2027, 1, 1),
                ]
            ),
            start=date(2025, 1, 1),
            maturity=date(2028, 1, 1),
        )
        .filter_between(
            start=date(2026, 1, 1),
            maturity=date(2026, 12, 31),
        )
        .normalize()
        .build()
    )

    assert schedule == [
        date(2026, 7, 1),
    ]


def test_builder_adjusts_dates_using_following_calendar():
    calendar = FakeCalendar(
        business_days=[
            date(2026, 1, 5),
            date(2026, 7, 1),
        ]
    )

    schedule = (
        ScheduleBuilder()
        .seed(
            rule=custom_dates(
                [
                    date(2026, 1, 3),
                    date(2026, 7, 1),
                ]
            ),
            start=date(2026, 1, 1),
            maturity=date(2026, 12, 31),
        )
        .adjust(following(calendar))
        .normalize()
        .build()
    )

    assert schedule == [
        date(2026, 1, 5),
        date(2026, 7, 1),
    ]


def test_builder_generates_semiannual_backward_schedule():
    schedule = (
        ScheduleBuilder()
        .seed(
            rule=every_n_months_backward(months=6),
            start=date(2026, 1, 10),
            maturity=date(2028, 1, 10),
        )
        .add_dates(date(2026, 1, 10))
        .normalize()
        .build()
    )

    assert schedule == [
        date(2026, 1, 10),
        date(2026, 7, 10),
        date(2027, 1, 10),
        date(2027, 7, 10),
        date(2028, 1, 10),
    ]


def test_builder_generates_first_day_january_july_schedule():
    schedule = (
        ScheduleBuilder()
        .seed(
            rule=first_day_of_months(months=[1, 7]),
            start=date(2026, 1, 1),
            maturity=date(2027, 1, 1),
        )
        .add_dates(date(2026, 1, 1))
        .normalize()
        .build()
    )

    assert schedule == [
        date(2026, 1, 1),
        date(2026, 7, 1),
        date(2027, 1, 1),
    ]