from datetime import date

import pytest

from engine_product.cashflows.models import Cashflow, CashflowType
from engine_product.pricing import YieldProblem


class FakeDayCount:
    def year_fraction(self, start: date, end: date) -> float:
        return 1.0


def make_single_cashflow(amount: float = 1100.0):
    return [
        Cashflow(
            payment_date=date(2027, 1, 1),
            amount=amount,
            cashflow_type=CashflowType.PRINCIPAL,
        )
    ]


def test_yield_problem_price_from_yield():
    problem = YieldProblem(
        cashflows=make_single_cashflow(1100.0),
        market_price=1000.0,
        settlement_date=date(2026, 1, 1),
        day_count=FakeDayCount(),
    )

    result = problem.price_from_yield(0.10)

    assert result == pytest.approx(1000.0)


def test_yield_problem_objective_is_zero_at_correct_yield():
    problem = YieldProblem(
        cashflows=make_single_cashflow(1100.0),
        market_price=1000.0,
        settlement_date=date(2026, 1, 1),
        day_count=FakeDayCount(),
    )

    result = problem.objective(0.10)

    assert result == pytest.approx(0.0, abs=1e-10)


def test_yield_problem_derivative_for_single_cashflow():
    problem = YieldProblem(
        cashflows=make_single_cashflow(1100.0),
        market_price=1000.0,
        settlement_date=date(2026, 1, 1),
        day_count=FakeDayCount(),
    )

    result = problem.derivative(0.10)

    expected = -1.0 * 1100.0 / (1.10 ** 2)

    assert result == pytest.approx(expected, abs=1e-10)


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

    assert problem.price_from_yield(0.10) == pytest.approx(1000.0, abs=1e-10)


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