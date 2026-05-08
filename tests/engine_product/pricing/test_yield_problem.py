from datetime import date

import pytest

from engine_product.cashflows.models import Cashflow, CashflowType
from engine_product.pricing import YieldProblem

ACCEPTED_ERROR = 1e-10

class FakeDayCount:
    def year_fraction(self, start: date, end: date) -> float:
        return 1.0


class MultiCashflowDayCount:
    def year_fraction(self, start: date, end: date) -> float:
        if end == date(2027, 1, 1):
            return 1.0

        if end == date(2028, 1, 1):
            return 2.0

        if end == date(2029, 1, 1):
            return 3.0

        raise ValueError(f"Unexpected payment date: {end}")


def make_single_cashflow(amount: float = 1100.0):
    return [
        Cashflow(
            payment_date=date(2027, 1, 1),
            amount=amount,
            cashflow_type=CashflowType.PRINCIPAL,
        )
    ]
    
def make_coupon_bond_cashflows():
    return [
        Cashflow(
            payment_date=date(2027, 1, 1),
            amount=50.0,
            cashflow_type=CashflowType.INTEREST,
        ),
        Cashflow(
            payment_date=date(2028, 1, 1),
            amount=50.0,
            cashflow_type=CashflowType.INTEREST,
        ),
        Cashflow(
            payment_date=date(2029, 1, 1),
            amount=1050.0,
            cashflow_type=CashflowType.PRINCIPAL,
        ),
    ]


def coupon_bond_price_at_10_percent() -> float:
    return (
        50.0 / (1.10 ** 1)
        + 50.0 / (1.10 ** 2)
        + 1050.0 / (1.10 ** 3)
    )


def test_yield_problem_price_from_yield():
    problem = YieldProblem(
        cashflows=make_single_cashflow(1100.0),
        market_price=1000.0,
        settlement_date=date(2026, 1, 1),
        day_count=FakeDayCount(),
    )

    result = problem.price_from_yield(0.10)

    assert result == pytest.approx(1000.0, abs=ACCEPTED_ERROR)

def test_yield_problem_price_from_yield_with_multiple_cashflows():
    problem = YieldProblem(
        cashflows=make_coupon_bond_cashflows(),
        market_price=coupon_bond_price_at_10_percent(),
        settlement_date=date(2026, 1, 1),
        day_count=MultiCashflowDayCount(),
    )

    result = problem.price_from_yield(0.10)

    assert result == pytest.approx(
        coupon_bond_price_at_10_percent(),
        abs=ACCEPTED_ERROR,
    )

def test_yield_problem_objective_is_zero_at_correct_yield():
    problem = YieldProblem(
        cashflows=make_single_cashflow(1100.0),
        market_price=1000.0,
        settlement_date=date(2026, 1, 1),
        day_count=FakeDayCount(),
    )

    result = problem.objective(0.10)

    assert result == pytest.approx(0.0, abs=ACCEPTED_ERROR)

def test_yield_problem_objective_is_zero_for_multiple_cashflows():
    problem = YieldProblem(
        cashflows=make_coupon_bond_cashflows(),
        market_price=coupon_bond_price_at_10_percent(),
        settlement_date=date(2026, 1, 1),
        day_count=MultiCashflowDayCount(),
    )

    result = problem.objective(0.10)

    assert result == pytest.approx(0.0, abs=ACCEPTED_ERROR)


def test_yield_problem_derivative_for_single_cashflow():
    problem = YieldProblem(
        cashflows=make_single_cashflow(1100.0),
        market_price=1000.0,
        settlement_date=date(2026, 1, 1),
        day_count=FakeDayCount(),
    )

    result = problem.derivative(0.10)

    expected = -1.0 * 1100.0 / (1.10 ** 2)

    assert result == pytest.approx(expected, abs=ACCEPTED_ERROR)

def test_yield_problem_derivative_with_multiple_cashflows():
    problem = YieldProblem(
        cashflows=make_coupon_bond_cashflows(),
        market_price=coupon_bond_price_at_10_percent(),
        settlement_date=date(2026, 1, 1),
        day_count=MultiCashflowDayCount(),
    )

    result = problem.derivative(0.10)

    expected = (
        -1.0 * 50.0 / (1.10 ** 2)
        -2.0 * 50.0 / (1.10 ** 3)
        -3.0 * 1050.0 / (1.10 ** 4)
    )

    assert result == pytest.approx(expected, abs=ACCEPTED_ERROR)

def test_yield_problem_time_amount_pairs_with_multiple_cashflows():
    problem = YieldProblem(
        cashflows=make_coupon_bond_cashflows(),
        market_price=coupon_bond_price_at_10_percent(),
        settlement_date=date(2026, 1, 1),
        day_count=MultiCashflowDayCount(),
    )

    assert problem.time_amount_pairs == [
        (1.0, 50.0),
        (2.0, 50.0),
        (3.0, 1050.0),
    ]

def test_yield_problem_ignores_past_cashflows():
    cashflows = [
        Cashflow(
            payment_date=date(2025, 1, 1),
            amount=9999.0,
            cashflow_type=CashflowType.INTEREST,
        ),
        Cashflow(
            payment_date=date(2027, 1, 1),
            amount=1100.0,
            cashflow_type=CashflowType.PRINCIPAL,
        ),
    ]

    problem = YieldProblem(
        cashflows=cashflows,
        market_price=1000.0,
        settlement_date=date(2026, 1, 1),
        day_count=FakeDayCount(),
    )

    assert problem.price_from_yield(0.10) == pytest.approx(1000.0, abs=ACCEPTED_ERROR)


def test_yield_problem_rejects_non_positive_market_price():
    with pytest.raises(ValueError, match="market_price must be positive"):
        YieldProblem(
            cashflows=make_single_cashflow(1100.0),
            market_price=0.0,
            settlement_date=date(2026, 1, 1),
            day_count=FakeDayCount(),
        )


def test_yield_problem_rejects_empty_future_cashflows():
    with pytest.raises(ValueError, match="cashflows must contain at least one future cashflow"):
        YieldProblem(
            cashflows=[
                Cashflow(
                    payment_date=date(2025, 1, 1),
                    amount=1000.0,
                    cashflow_type=CashflowType.PRINCIPAL,
                )
            ],
            market_price=1000.0,
            settlement_date=date(2026, 1, 1),
            day_count=FakeDayCount(),
        )


def test_yield_problem_rejects_yield_less_than_minus_one():
    problem = YieldProblem(
        cashflows=make_single_cashflow(1100.0),
        market_price=1000.0,
        settlement_date=date(2026, 1, 1),
        day_count=FakeDayCount(),
    )

    with pytest.raises(ValueError, match="ytm must be greater than -1"):
        problem.price_from_yield(-1.0)