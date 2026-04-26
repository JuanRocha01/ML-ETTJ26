from datetime import date

from engine_product.schedules.rules import (
    custom_dates,
    every_n_months_backward,
    first_day_of_months,
    nth_weekday_of_month,
)


def test_custom_dates_filters_between_start_and_maturity():
    rule = custom_dates(
        [
            date(2025, 12, 31),
            date(2026, 1, 10),
            date(2026, 7, 10),
            date(2027, 1, 10),
        ]
    )

    result = rule(
        start=date(2026, 1, 1),
        maturity=date(2026, 12, 31),
    )

    assert result == [
        date(2026, 1, 10),
        date(2026, 7, 10),
    ]


def test_every_n_months_backward_generates_dates_from_maturity():
    rule = every_n_months_backward(months=6)

    result = rule(
        start=date(2026, 1, 10),
        maturity=date(2028, 1, 10),
    )

    assert result == [
        date(2028, 1, 10),
        date(2027, 7, 10),
        date(2027, 1, 10),
        date(2026, 7, 10),
    ]


def test_every_n_months_backward_rejects_non_positive_months():
    try:
        every_n_months_backward(months=0)
    except ValueError as exc:
        assert str(exc) == "months must be positive"
    else:
        raise AssertionError("Expected ValueError")


def test_first_day_of_months_generates_january_and_july():
    rule = first_day_of_months(months=[1, 7])

    result = rule(
        start=date(2026, 1, 1),
        maturity=date(2027, 1, 1),
    )

    assert result == [
        date(2026, 7, 1),
        date(2027, 1, 1),
    ]


def test_nth_weekday_of_month_generates_third_wednesday():
    rule = nth_weekday_of_month(
        months=[1],
        weekday=2,
        n=3,
    )

    result = rule(
        start=date(2026, 1, 1),
        maturity=date(2026, 1, 31),
    )

    assert result == [date(2026, 1, 21)]


def test_nth_weekday_of_month_rejects_invalid_weekday():
    try:
        nth_weekday_of_month(months=[1], weekday=7, n=3)
    except ValueError as exc:
        assert str(exc) == "weekday must be between 0 and 6"
    else:
        raise AssertionError("Expected ValueError")


def test_nth_weekday_of_month_rejects_non_positive_n():
    try:
        nth_weekday_of_month(months=[1], weekday=2, n=0)
    except ValueError as exc:
        assert str(exc) == "n must be positive"
    else:
        raise AssertionError("Expected ValueError")