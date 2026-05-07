from datetime import date

import pytest

from engine_product.pricing import zero_coupon_yield


class FakeDayCount:
    def year_fraction(self, start: date, end: date) -> float:
        return 1.0


def test_zero_coupon_yield_solves_closed_form_yield():
    result = zero_coupon_yield(
        price=900.0,
        notional=1000.0,
        settlement_date=date(2026, 1, 1),
        maturity_date=date(2027, 1, 1),
        day_count=FakeDayCount(),
    )

    expected = (1000.0 / 900.0) - 1.0

    assert result == expected


def test_zero_coupon_yield_rejects_non_positive_price():
    with pytest.raises(ValueError, match="price must be positive"):
        zero_coupon_yield(
            price=0.0,
            notional=1000.0,
            settlement_date=date(2026, 1, 1),
            maturity_date=date(2027, 1, 1),
            day_count=FakeDayCount(),
        )


def test_zero_coupon_yield_rejects_non_positive_notional():
    with pytest.raises(ValueError, match="notional must be positive"):
        zero_coupon_yield(
            price=900.0,
            notional=0.0,
            settlement_date=date(2026, 1, 1),
            maturity_date=date(2027, 1, 1),
            day_count=FakeDayCount(),
        )


def test_zero_coupon_yield_rejects_maturity_before_settlement():
    with pytest.raises(ValueError, match="maturity_date must be after settlement_date"):
        zero_coupon_yield(
            price=900.0,
            notional=1000.0,
            settlement_date=date(2027, 1, 1),
            maturity_date=date(2026, 1, 1),
            day_count=FakeDayCount(),
        )