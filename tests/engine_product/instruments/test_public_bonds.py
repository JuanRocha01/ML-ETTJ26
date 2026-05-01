# tests/engine_product/instruments/test_public_bonds.py

from datetime import date, timedelta

import pytest

from engine_product.cashflows.models import CashflowType
from engine_product.instruments.public_bonds import (
    DEFAULT_NTNF_COUPON_RATE,
    DEFAULT_PUBLIC_BOND_NOTIONAL,
    LTNContract,
    NTNFContract,
)


class FakeCalendar:
    def __init__(self, holidays=None):
        self.holidays = set(holidays or [])

    def is_business_day(self, d: date) -> bool:
        return d.weekday() < 5 and d not in self.holidays

    def adjust_next_business_day(self, d: date) -> date:
        current = d

        while not self.is_business_day(current):
            current += timedelta(days=1)

        return current

    def adjust_previous_business_day(self, d: date) -> date:
        current = d

        while not self.is_business_day(current):
            current -= timedelta(days=1)

        return current


class FakeDayCount:
    def year_fraction(self, start: date, end: date) -> float:
        return 0.5


def test_ltn_uses_default_notional():
    contract = LTNContract(
        start_date=date(2026, 1, 2),
        maturity_date=date(2027, 1, 4),
        calendar=FakeCalendar(),
    )

    assert contract.notional == DEFAULT_PUBLIC_BOND_NOTIONAL


def test_ltn_build_schedule_returns_only_adjusted_maturity_date():
    contract = LTNContract(
        start_date=date(2026, 1, 2),
        maturity_date=date(2027, 1, 2),
        calendar=FakeCalendar(),
    )

    result = contract.build_schedule()

    assert result == [date(2027, 1, 4)]


def test_ltn_build_events_returns_single_principal_event():
    contract = LTNContract(
        start_date=date(2026, 1, 2),
        maturity_date=date(2027, 1, 2),
        calendar=FakeCalendar(),
    )

    events = contract.build_events()

    assert len(events) == 1
    assert events[0].event_date == date(2027, 1, 4)
    assert events[0].interest is False
    assert events[0].principal is True


def test_ltn_build_cashflows_returns_single_principal_cashflow():
    contract = LTNContract(
        start_date=date(2026, 1, 2),
        maturity_date=date(2027, 1, 2),
        calendar=FakeCalendar(),
    )

    cashflows = contract.build_cashflows(
        as_of_date=date(2026, 3, 1),
    )

    assert len(cashflows) == 1

    cf = cashflows[0]

    assert cf.payment_date == date(2027, 1, 4)
    assert cf.cashflow_type == CashflowType.PRINCIPAL
    assert cf.amount == pytest.approx(1000.0)
    assert cf.notional_before == pytest.approx(1000.0)
    assert cf.notional_after == pytest.approx(0.0)


def test_ltn_build_cashflows_returns_empty_when_maturity_is_before_as_of_date():
    contract = LTNContract(
        start_date=date(2026, 1, 2),
        maturity_date=date(2027, 1, 2),
        calendar=FakeCalendar(),
    )

    cashflows = contract.build_cashflows(
        as_of_date=date(2027, 1, 5),
    )

    assert cashflows == []


def test_ntnf_uses_default_notional_and_coupon_rate():
    contract = NTNFContract(
        start_date=date(2026, 1, 2),
        maturity_date=date(2027, 1, 1),
        calendar=FakeCalendar(),
        day_count=FakeDayCount(),
    )

    assert contract.notional == DEFAULT_PUBLIC_BOND_NOTIONAL
    assert contract.coupon_rate == DEFAULT_NTNF_COUPON_RATE


def test_ntnf_build_schedule_uses_first_business_days_of_january_and_july():
    contract = NTNFContract(
        start_date=date(2026, 1, 2),
        maturity_date=date(2027, 1, 1),
        calendar=FakeCalendar(holidays={date(2027, 1, 1)}),
        day_count=FakeDayCount(),
    )

    result = contract.build_schedule()

    assert result == [
        date(2026, 1, 2),
        date(2026, 7, 1),
        date(2027, 1, 4),
    ]


def test_ntnf_build_events_generates_interest_events_and_principal_at_maturity():
    contract = NTNFContract(
        start_date=date(2026, 1, 2),
        maturity_date=date(2027, 1, 1),
        calendar=FakeCalendar(holidays={date(2027, 1, 1)}),
        day_count=FakeDayCount(),
    )

    events = contract.build_events()

    assert len(events) == 2

    first_event = events[0]
    last_event = events[1]

    assert first_event.event_date == date(2026, 7, 1)
    assert first_event.interest is True
    assert first_event.principal is False

    assert last_event.event_date == date(2027, 1, 4)
    assert last_event.interest is True
    assert last_event.principal is True


def test_ntnf_build_cashflows_generates_interest_and_principal_cashflows():
    contract = NTNFContract(
        start_date=date(2026, 1, 2),
        maturity_date=date(2027, 1, 1),
        calendar=FakeCalendar(holidays={date(2027, 1, 1)}),
        day_count=FakeDayCount(),
    )

    cashflows = contract.build_cashflows(
        as_of_date=date(2026, 3, 1),
    )

    coupon_amount = 1000.0 * ((1.0 + 0.10) ** 0.5 - 1.0)

    interest_cashflows = [
        cf for cf in cashflows
        if cf.cashflow_type == CashflowType.INTEREST
    ]

    principal_cashflows = [
        cf for cf in cashflows
        if cf.cashflow_type == CashflowType.PRINCIPAL
    ]

    assert len(interest_cashflows) == 2
    assert len(principal_cashflows) == 1

    assert interest_cashflows[0].payment_date == date(2026, 7, 1)
    assert interest_cashflows[0].amount == pytest.approx(coupon_amount)

    assert interest_cashflows[1].payment_date == date(2027, 1, 4)
    assert interest_cashflows[1].amount == pytest.approx(coupon_amount)

    assert principal_cashflows[0].payment_date == date(2027, 1, 4)
    assert principal_cashflows[0].amount == pytest.approx(1000.0)


def test_ntnf_allows_overriding_notional_and_coupon_rate():
    contract = NTNFContract(
        start_date=date(2026, 1, 2),
        maturity_date=date(2027, 1, 1),
        calendar=FakeCalendar(),
        day_count=FakeDayCount(),
        notional=2_000.0,
        coupon_rate=0.12,
    )

    cashflows = contract.build_cashflows(
        as_of_date=date(2026, 3, 1),
    )

    coupon_amount = 2_000.0 * ((1.0 + 0.12) ** 0.5 - 1.0)

    interest_cashflows = [
        cf for cf in cashflows
        if cf.cashflow_type == CashflowType.INTEREST
    ]

    principal_cashflows = [
        cf for cf in cashflows
        if cf.cashflow_type == CashflowType.PRINCIPAL
    ]

    assert interest_cashflows[0].amount == pytest.approx(coupon_amount)
    assert principal_cashflows[0].amount == pytest.approx(2_000.0)