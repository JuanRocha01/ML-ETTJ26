from datetime import date

from engine_product.convention.conventions import BU252


class FakeBusinessCalendar:
    def business_days_between(self, start, end):
        return 2


def test_bu252_day_count():
    bu252 = BU252(FakeBusinessCalendar())

    result = bu252.day_count(date(2026, 1, 1), date(2026, 1, 5))

    assert result == 2


def test_bu252_year_fraction():
    bu252 = BU252(FakeBusinessCalendar())

    result = bu252.year_fraction(date(2026, 1, 1), date(2026, 1, 5))

    assert result == 2 / 252