from datetime import date

import pytest

from engine_product.cashflows.models import Cashflow, CashflowType
from engine_product.pricing import YieldProblem
from engine_product.pricing.yield_solvers import YieldSolverMethod, yield_to_maturity_batch
from engine_product.pricing.yield_solvers_batch import (
    BatchNewtonConfig,
    BatchYieldSolver,
)

ACCEPTED_ERROR = 1e-10

class AlwaysFailSolver:
    def solve(self, problem):
        raise RuntimeError("forced failure")

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

class TrackingBatchYieldSolver(BatchYieldSolver):
    def __init__(self):
        super().__init__()
        object.__setattr__(self, "group_sizes", [])

    def _solve_multi_cashflows_with_newton_batch(self, items, results):
        self.group_sizes.append(len(items[0][1].time_amount_pairs))
        return super()._solve_multi_cashflows_with_newton_batch(items, results)


def make_single_problem(amount=1100.0, market_price=1000.0):
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


def make_coupon_bond_cashflows():
    return [
        Cashflow(date(2027, 1, 1), 50.0, CashflowType.INTEREST),
        Cashflow(date(2028, 1, 1), 50.0, CashflowType.INTEREST),
        Cashflow(date(2029, 1, 1), 1050.0, CashflowType.PRINCIPAL),
    ]


def coupon_bond_price_at_10_percent():
    return (
        50.0 / (1.10**1)
        + 50.0 / (1.10**2)
        + 1050.0 / (1.10**3)
    )


def make_coupon_problem():
    return YieldProblem(
        cashflows=make_coupon_bond_cashflows(),
        market_price=coupon_bond_price_at_10_percent(),
        settlement_date=date(2026, 1, 1),
        day_count=MultiCashflowDayCount(),
    )


def test_batch_vectorizes_single_cashflow_problems():
    results = yield_to_maturity_batch(
        [
            make_single_problem(amount=1100.0, market_price=1000.0),
            make_single_problem(amount=1210.0, market_price=1000.0),
        ]
    )

    assert [item.succeeded for item in results] == [True, True]
    assert [item.result.method for item in results] == [
        YieldSolverMethod.ZERO_COUPON,
        YieldSolverMethod.ZERO_COUPON,
    ]
    assert [item.result.iterations for item in results] == [0, 0]
    assert [item.result.ytm for item in results] == pytest.approx(
        [0.10, 0.21],
        abs=ACCEPTED_ERROR,
    )


def test_batch_solves_multi_cashflow_with_newton_batch():
    results = yield_to_maturity_batch([make_coupon_problem()])

    assert len(results) == 1
    assert results[0].succeeded is True
    assert results[0].result.method == YieldSolverMethod.NEWTON_BATCH
    assert results[0].result.ytm == pytest.approx(0.10, abs=ACCEPTED_ERROR)
    assert results[0].result.iterations is not None


def test_batch_preserves_original_indexes_for_mixed_inputs():
    results = yield_to_maturity_batch(
        [
            make_single_problem(),
            make_coupon_problem(),
            make_single_problem(amount=1210.0, market_price=1000.0),
        ]
    )

    assert [item.index for item in results] == [0, 1, 2]
    assert results[0].result.method == YieldSolverMethod.ZERO_COUPON
    assert results[1].result.method == YieldSolverMethod.NEWTON_BATCH
    assert results[2].result.method == YieldSolverMethod.ZERO_COUPON


def test_batch_keeps_processing_after_single_cashflow_failure():
    invalid = YieldProblem.from_time_amount_pairs(
        time_amount_pairs=[(1.0, -100.0)],
        market_price=1000.0,
    )

    results = yield_to_maturity_batch(
        [
            invalid,
            make_single_problem(),
        ]
    )

    assert results[0].succeeded is False
    assert results[0].error_type == "ValueError"
    assert results[1].succeeded is True
    assert results[1].result.ytm == pytest.approx(0.10, abs=ACCEPTED_ERROR)


def test_batch_falls_back_to_unit_solver_when_newton_batch_fails():
    solver = BatchYieldSolver(
        newton=BatchNewtonConfig(
            initial_guess=9.0,
            lower=-0.95,
            upper=10.0,
            tol=1e-12,
            price_tol=1e-10,
            maxiter=1,
        )
    )

    results = solver.solve_many([make_coupon_problem()])

    assert len(results) == 1
    assert results[0].succeeded is True
    assert results[0].result.ytm == pytest.approx(0.10, abs=ACCEPTED_ERROR)
    assert results[0].result.method in {
        YieldSolverMethod.NEWTON,
        YieldSolverMethod.BRENT,
        YieldSolverMethod.BRENT_EXPANDED,
    }


def test_batch_returns_individual_failure_when_batch_and_unit_solver_fail():
    impossible = YieldProblem.from_time_amount_pairs(
        time_amount_pairs=[(1.0, -100.0), (2.0, -100.0)],
        market_price=1000.0,
    )

    solver = BatchYieldSolver(unit_solver=AlwaysFailSolver())

    results = solver.solve_many([impossible])

    assert len(results) == 1
    assert results[0].succeeded is False
    assert results[0].error_type == "RuntimeError"
    assert results[0].error_message == "forced failure"

def test_batch_groups_multi_cashflows_by_cashflow_count():
    two_cashflow_problem = YieldProblem.from_time_amount_pairs(
        time_amount_pairs=[
            (1.0, 50.0),
            (2.0, 1050.0),
        ],
        market_price=(
            50.0 / (1.10**1)
            + 1050.0 / (1.10**2)
        ),
    )

    three_cashflow_problem = YieldProblem.from_time_amount_pairs(
        time_amount_pairs=[
            (1.0, 50.0),
            (2.0, 50.0),
            (3.0, 1050.0),
        ],
        market_price=(
            50.0 / (1.10**1)
            + 50.0 / (1.10**2)
            + 1050.0 / (1.10**3)
        ),
    )

    results = yield_to_maturity_batch(
        [
            two_cashflow_problem,
            three_cashflow_problem,
        ]
    )

    assert [item.index for item in results] == [0, 1]
    assert [item.succeeded for item in results] == [True, True]

    assert results[0].result.method == YieldSolverMethod.NEWTON_BATCH
    assert results[1].result.method == YieldSolverMethod.NEWTON_BATCH

    assert results[0].result.ytm == pytest.approx(0.10, abs=ACCEPTED_ERROR)
    assert results[1].result.ytm == pytest.approx(0.10, abs=ACCEPTED_ERROR)

def test_batch_newton_splits_groups_by_cashflow_count():
    two_cashflow_problem = YieldProblem.from_time_amount_pairs(
        time_amount_pairs=[
            (1.0, 50.0),
            (2.0, 1050.0),
        ],
        market_price=50.0 / (1.10**1) + 1050.0 / (1.10**2),
    )

    three_cashflow_problem = YieldProblem.from_time_amount_pairs(
        time_amount_pairs=[
            (1.0, 50.0),
            (2.0, 50.0),
            (3.0, 1050.0),
        ],
        market_price=50.0 / (1.10**1) + 50.0 / (1.10**2) + 1050.0 / (1.10**3),
    )

    solver = TrackingBatchYieldSolver()

    results = solver.solve_many(
        [
            two_cashflow_problem,
            three_cashflow_problem,
        ]
    )

    assert solver.group_sizes == [2, 3]
    assert [item.succeeded for item in results] == [True, True]
    assert [item.result.method for item in results] == [
        YieldSolverMethod.NEWTON_BATCH,
        YieldSolverMethod.NEWTON_BATCH,
    ]
