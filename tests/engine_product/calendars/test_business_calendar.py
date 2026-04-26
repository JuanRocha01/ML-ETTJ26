from datetime import date


def test_business_calendar_delegates_is_business_day(business_calendar):
    assert business_calendar.is_business_day(date(2026, 1, 2)) is True


def test_business_calendar_delegates_business_days_between(business_calendar):
    result = business_calendar.business_days_between(
        date(2026, 1, 1),
        date(2026, 1, 5),
    )

    assert result == 2


def test_business_calendar_delegates_adjust_to_next_business_day(business_calendar):
    result = business_calendar.adjust_to_next_business_day(date(2026, 1, 3))

    assert result == date(2026, 1, 5)