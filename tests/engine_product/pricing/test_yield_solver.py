# tests/engine_product/pricing/test_yield_solvers.py

from datetime import date

import pytest

from engine_product.cashflows.models import Cashflow, CashflowType
from engine_product.pricing import YieldProblem
from engine_product.pricing.yield_solvers import (
    BrentYieldSolver,
    ExpandedBrentYieldSolver,
    FallbackYieldSolver,
    NewtonYieldSolver,
    YieldSolverMethod,
    yield_to_maturity,
)


class FakeDayCount:
    def year_fraction(self, start: date, end: date) -> float:
        return 1.0

class AlwaysFailSolver:
    def solve(self, problem):
        raise RuntimeError("forced failure")


def make_problem(
    amount: float = 1100.0,
    market_price: float = 1000.0,
) -> YieldProblem:
    return YieldProblem(
        cashflows=[
            Cashflow(
                payment_date=date(2027, 1, 1),
                amount=amount,
                cashflow_type=CashflowType.PRINCIPAL,
            )
        ],
        market_price=market_price,
        settlement_date=date(2026, 1, 1),
        day_count=FakeDayCount(),
    )

def test_newton_solver_solves_simple_problem():
    problem = make_problem()

    solver = NewtonYieldSolver(
        initial_guess=0.05,
        lower=-0.95,
        upper=10.0,
    )

    result = solver.solve(problem)

    assert result.ytm == pytest.approx(0.10, abs=1e-10)
    assert result.method == YieldSolverMethod.NEWTON
    assert result.iterations is not None

def test_brent_solver_solves_simple_problem():
    problem = make_problem()

    solver = BrentYieldSolver(
        lower=-0.95,
        upper=1.50,
    )

    result = solver.solve(problem)

    assert result.ytm == pytest.approx(0.10, abs=1e-10)
    assert result.method == YieldSolverMethod.BRENT
    assert result.iterations is not None

def test_brent_solver_fails_when_root_is_not_bracketed():
    problem = make_problem(
        amount=1100.0,
        market_price=100.0,
    )

    solver = BrentYieldSolver(
        lower=-0.95,
        upper=1.50,
    )

    with pytest.raises(RuntimeError, match="Brent root is not bracketed"):
        solver.solve(problem)

def test_expanded_brent_solver_expands_upper_bound_and_solves():
    problem = make_problem(
        amount=1100.0,
        market_price=100.0,
    )

    solver = ExpandedBrentYieldSolver(
        lower=-0.95,
        initial_upper=1.50,
        max_upper=20.0,
        expansion_factor=2.0,
    )

    result = solver.solve(problem)

    expected = 1100.0 / 100.0 - 1.0

    assert result.ytm == pytest.approx(expected, abs=1e-10)
    assert result.method == YieldSolverMethod.BRENT_EXPANDED

def test_fallback_solver_uses_newton_when_newton_converges():
    problem = make_problem()

    solver = FallbackYieldSolver(
        solvers=[
            NewtonYieldSolver(initial_guess=0.05),
            BrentYieldSolver(),
            ExpandedBrentYieldSolver(),
        ]
    )

    result = solver.solve(problem)

    assert result.ytm == pytest.approx(0.10, abs=1e-10 )
    assert result.method == YieldSolverMethod.NEWTON

def test_fallback_solver_uses_brent_after_previous_solver_fails():
    problem = make_problem()

    solver = FallbackYieldSolver(
        solvers=[
            AlwaysFailSolver(),
            BrentYieldSolver(lower=-0.95, upper=1.50),
        ]
    )

    result = solver.solve(problem)

    assert result.ytm == pytest.approx(0.10, abs=1e-10)
    assert result.method == YieldSolverMethod.BRENT

def test_fallback_solver_uses_expanded_brent_after_brent_fails():
    problem = make_problem(
        amount=1100.0,
        market_price=100.0,
    )

    solver = FallbackYieldSolver(
        solvers=[
            AlwaysFailSolver(),
            BrentYieldSolver(lower=-0.95, upper=1.50),
            ExpandedBrentYieldSolver(
                lower=-0.95,
                initial_upper=1.50,
                max_upper=20.0,
                expansion_factor=2.0,
            ),
        ]
    )

    result = solver.solve(problem)

    expected = 1100.0 / 100.0 - 1.0

    assert result.ytm == pytest.approx(expected, abs=1e-10)
    assert result.method == YieldSolverMethod.BRENT_EXPANDED

def test_fallback_solver_raises_error_when_all_solvers_fail():
    problem = make_problem()

    solver = FallbackYieldSolver(
        solvers=[
            AlwaysFailSolver(),
            AlwaysFailSolver(),
        ]
    )

    with pytest.raises(RuntimeError, match="All yield solvers failed"):
        solver.solve(problem)

def test_yield_to_maturity_uses_default_solver_chain():
    problem = make_problem()

    result = yield_to_maturity(problem)

    assert result.ytm == pytest.approx(0.10, abs=1e-10)
    assert result.method in {
        YieldSolverMethod.NEWTON,
        YieldSolverMethod.BRENT,
        YieldSolverMethod.BRENT_EXPANDED,
    }