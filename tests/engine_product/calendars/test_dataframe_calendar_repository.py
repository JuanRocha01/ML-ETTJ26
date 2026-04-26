from datetime import date


def test_get_returns_full_row(calendar_repo):
    row = calendar_repo.get(date(2026, 1, 2))

    assert row["calendar_id"] == "BR_ANBIMA"
    assert row["is_business_day"] is True
    assert row["bd_index"] == 1


def test_is_business_day_returns_true_for_business_day(calendar_repo):
    assert calendar_repo.is_business_day(date(2026, 1, 2)) is True


def test_is_business_day_returns_false_for_holiday(calendar_repo):
    assert calendar_repo.is_business_day(date(2026, 1, 1)) is False


def test_is_business_day_returns_false_for_weekend(calendar_repo):
    assert calendar_repo.is_business_day(date(2026, 1, 3)) is False


def test_actual_days_between(calendar_repo):
    result = calendar_repo.actual_days_between(
        date(2026, 1, 1),
        date(2026, 1, 5),
    )

    assert result == 4


def test_business_days_between(calendar_repo):
    result = calendar_repo.business_days_between(
        date(2026, 1, 1),
        date(2026, 1, 5),
    )

    assert result == 2


def test_adjust_to_next_business_day_when_date_is_holiday(calendar_repo):
    result = calendar_repo.adjust_to_next_business_day(date(2026, 1, 1))

    assert result == date(2026, 1, 2)


def test_adjust_to_next_business_day_when_date_is_weekend(calendar_repo):
    result = calendar_repo.adjust_to_next_business_day(date(2026, 1, 3))

    assert result == date(2026, 1, 5)


def test_adjust_to_next_business_day_when_date_is_already_business_day(calendar_repo):
    result = calendar_repo.adjust_to_next_business_day(date(2026, 1, 2))

    assert result == date(2026, 1, 2)


def test_first_business_day_of_month(calendar_repo):
    result = calendar_repo.first_business_day_of_month(2026, 1)

    assert result == date(2026, 1, 2)