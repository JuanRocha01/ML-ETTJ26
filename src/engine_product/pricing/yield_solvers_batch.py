from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np

from engine_product.pricing.yield_problem import YieldProblem
from engine_product.pricing.yield_solvers import (
    YieldSolver,
    YieldSolverMethod,
    YieldSolverResult,
    default_yield_solver,
)


@dataclass(frozen=True)
class YieldSolverBatchResult:
    index: int
    result: YieldSolverResult | None = None
    error_type: str | None = None
    error_message: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.result is not None


@dataclass(frozen=True)
class BatchNewtonConfig:
    initial_guess: float = 0.10
    lower: float = -0.95
    upper: float = 10.0
    tol: float = 1e-12
    price_tol: float = 1e-10
    maxiter: int = 35
    min_abs_derivative: float = 1e-14


@dataclass(frozen=True)
class BatchYieldSolver:
    unit_solver: YieldSolver | None = None
    newton: BatchNewtonConfig = BatchNewtonConfig()

    def solve_many(
        self,
        problems: Iterable[YieldProblem],
    ) -> list[YieldSolverBatchResult]:
        problems = list(problems)
        unit_solver = self.unit_solver or default_yield_solver()

        results: list[YieldSolverBatchResult | None] = [None] * len(problems)

        single_items: list[tuple[int, YieldProblem]] = []
        multi_items_by_size: dict[int, list[tuple[int, YieldProblem]]] = {}

        for index, problem in enumerate(problems):
            if problem.is_single_cashflow:
                single_items.append((index, problem))
            else:
                size = len(problem.time_amount_pairs)
                multi_items_by_size.setdefault(size, []).append((index, problem))

        self._solve_single_cashflows(single_items, results)

        for items in multi_items_by_size.values():
            failed_indexes = self._solve_multi_cashflows_with_newton_batch(
                items=items,
                results=results,
            )

            for index in failed_indexes:
                self._solve_with_unit_solver(
                    index=index,
                    problem=problems[index],
                    solver=unit_solver,
                    results=results,
                )

        return [result for result in results if result is not None]

    def _solve_single_cashflows(
        self,
        items: list[tuple[int, YieldProblem]],
        results: list[YieldSolverBatchResult | None],
    ) -> None:
        if not items:
            return

        indexes = [index for index, _ in items]
        pairs = [problem.time_amount_pairs[0] for _, problem in items]

        times = np.asarray([t for t, _ in pairs], dtype=float)
        amounts = np.asarray([amount for _, amount in pairs], dtype=float)
        prices = np.asarray([problem.market_price for _, problem in items], dtype=float)

        valid = (times > 0.0) & (amounts > 0.0) & (prices > 0.0)

        ytms = np.full(len(items), np.nan, dtype=float)
        ytms[valid] = np.power(
            amounts[valid] / prices[valid],
            1.0 / times[valid],
        ) - 1.0

        accepted = valid & np.isfinite(ytms)

        for pos, index in enumerate(indexes):
            if accepted[pos]:
                results[index] = YieldSolverBatchResult(
                    index=index,
                    result=YieldSolverResult(
                        ytm=float(ytms[pos]),
                        method=YieldSolverMethod.ZERO_COUPON,
                        iterations=0,
                    ),
                )
            else:
                results[index] = YieldSolverBatchResult(
                    index=index,
                    error_type="ValueError",
                    error_message=(
                        "single cashflow batch solve requires positive time, "
                        "positive amount, positive market_price, and finite yield"
                    ),
                )

    def _solve_multi_cashflows_with_newton_batch(
        self,
        items: list[tuple[int, YieldProblem]],
        results: list[YieldSolverBatchResult | None],
    ) -> list[int]:
        indexes = [index for index, _ in items]
        problems = [problem for _, problem in items]

        times = np.asarray(
            [[t for t, _ in problem.time_amount_pairs] for problem in problems],
            dtype=float,
        )
        amounts = np.asarray(
            [[amount for _, amount in problem.time_amount_pairs] for problem in problems],
            dtype=float,
        )
        prices = np.asarray([problem.market_price for problem in problems], dtype=float)

        n = len(problems)
        y = np.full(n, self.newton.initial_guess, dtype=float)
        iterations = np.zeros(n, dtype=int)

        active = np.ones(n, dtype=bool)
        converged = np.zeros(n, dtype=bool)
        failed = np.zeros(n, dtype=bool)

        for iteration in range(1, self.newton.maxiter + 1):
            active_idx = active & ~converged & ~failed

            if not np.any(active_idx):
                break

            y_active = y[active_idx]
            times_active = times[active_idx]
            amounts_active = amounts[active_idx]
            prices_active = prices[active_idx]

            with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
                base = 1.0 + y_active[:, None]

                model_prices = (
                    amounts_active / np.power(base, times_active)
                ).sum(axis=1)

                objective = model_prices - prices_active

                derivative = (
                    -times_active
                    * amounts_active
                    / np.power(base, times_active + 1.0)
                ).sum(axis=1)

                y_next = y_active - objective / derivative

            local_failed = (
                ~np.isfinite(objective)
                | ~np.isfinite(derivative)
                | ~np.isfinite(y_next)
                | (np.abs(derivative) < self.newton.min_abs_derivative)
                | (y_next <= self.newton.lower)
                | (y_next >= self.newton.upper)
            )

            local_converged = (
                ~local_failed
                & (
                    (np.abs(objective) <= self.newton.price_tol)
                    | (np.abs(y_next - y_active) <= self.newton.tol)
                )
            )

            positions = np.flatnonzero(active_idx)

            y[positions[~local_failed]] = y_next[~local_failed]
            iterations[positions] = iteration

            converged[positions[local_converged]] = True
            failed[positions[local_failed]] = True

            active = ~(converged | failed)

        failed[~converged & ~failed] = True

        for pos, index in enumerate(indexes):
            if converged[pos]:
                results[index] = YieldSolverBatchResult(
                    index=index,
                    result=YieldSolverResult(
                        ytm=float(y[pos]),
                        method=YieldSolverMethod.NEWTON_BATCH,
                        iterations=int(iterations[pos]),
                    ),
                )

        return [indexes[pos] for pos in range(n) if failed[pos]]

    def _solve_with_unit_solver(
        self,
        index: int,
        problem: YieldProblem,
        solver: YieldSolver,
        results: list[YieldSolverBatchResult | None],
    ) -> None:
        try:
            result = solver.solve(problem)
            results[index] = YieldSolverBatchResult(index=index, result=result)
        except Exception as exc:
            results[index] = YieldSolverBatchResult(
                index=index,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
