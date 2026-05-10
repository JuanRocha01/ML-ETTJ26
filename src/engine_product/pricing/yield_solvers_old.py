from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from scipy.optimize import brentq, newton

from engine_product.pricing.yield_problem import YieldProblem


class YieldSolverMethod(str, Enum):
    ZERO_COUPON = "ZERO_COUPON"
    NEWTON = "NEWTON"
    BRENT = "BRENT"
    BRENT_EXPANDED = "BRENT_EXPANDED"



@dataclass(frozen=True)
class YieldSolverResult:
    ytm: float
    method: YieldSolverMethod
    iterations: int | None = None

class YieldSolver(Protocol):
    def solve(self, problem: YieldProblem) -> YieldSolverResult:
        ...

@dataclass(frozen=True)
class FallbackYieldSolver:
    solvers: list[YieldSolver]

    def solve(self, problem: YieldProblem) -> YieldSolverResult:
        errors: list[str] = []

        for solver in self.solvers:
            try:
                return solver.solve(problem)
            except Exception as exc:
                errors.append(
                    f"{solver.__class__.__name__}: {exc}"
                )

        raise RuntimeError(
            "All yield solvers failed. "
            + " | ".join(errors)
        )

@dataclass(frozen=True)
class NewtonYieldSolver:
    initial_guess: float = 0.10
    lower: float = -0.95
    upper: float = 10.0
    tol: float = 1e-12
    maxiter: int = 35

    def solve(self, problem: YieldProblem) -> YieldSolverResult:
        root, info = newton(
            func=problem.objective,
            x0=self.initial_guess,
            fprime=problem.derivative,
            tol=self.tol,
            maxiter=self.maxiter,
            full_output=True,
            disp=False,
        )

        if not info.converged:
            raise RuntimeError("Newton did not converge")

        if not self.lower < root < self.upper:
            raise RuntimeError(f"Newton root outside accepted range: {root}")

        return YieldSolverResult(
            ytm=float(root),
            method=YieldSolverMethod.NEWTON,
            iterations=info.iterations,
        )

@dataclass(frozen=True)
class BrentYieldSolver:
    lower: float = -0.95
    upper: float = 1.50
    xtol: float = 1e-12
    rtol: float = 1e-12
    maxiter: int = 100

    def solve(self, problem: YieldProblem) -> YieldSolverResult:
        f_lower = problem.objective(self.lower)
        f_upper = problem.objective(self.upper)

        if f_lower == 0:
            return YieldSolverResult(
                ytm=self.lower,
                method=YieldSolverMethod.BRENT,
                iterations=0,
            )

        if f_upper == 0:
            return YieldSolverResult(
                ytm=self.upper,
                method=YieldSolverMethod.BRENT,
                iterations=0,
            )

        if f_lower * f_upper > 0:
            raise RuntimeError(
                "Brent root is not bracketed. "
                f"objective({self.lower})={f_lower}, "
                f"objective({self.upper})={f_upper}"
            )

        root, info = brentq(
            problem.objective,
            self.lower,
            self.upper,
            xtol=self.xtol,
            rtol=self.rtol,
            maxiter=self.maxiter,
            full_output=True,
            disp=False,
        )

        return YieldSolverResult(
            ytm=float(root),
            method=YieldSolverMethod.BRENT,
            iterations=info.iterations,
        )

@dataclass(frozen=True)
class ExpandedBrentYieldSolver:
    lower: float = -0.95
    initial_upper: float = 1.50
    max_upper: float = 10.0
    expansion_factor: float = 2.0
    xtol: float = 1e-12
    rtol: float = 1e-12
    maxiter: int = 100

    def solve(self, problem: YieldProblem) -> YieldSolverResult:
        f_lower = problem.objective(self.lower)

        current_upper = self.initial_upper

        while current_upper <= self.max_upper:
            f_upper = problem.objective(current_upper)

            if f_lower == 0:
                return YieldSolverResult(
                    ytm=self.lower,
                    method=YieldSolverMethod.BRENT_EXPANDED,
                    iterations=0,
                )

            if f_upper == 0:
                return YieldSolverResult(
                    ytm=current_upper,
                    method=YieldSolverMethod.BRENT_EXPANDED,
                    iterations=0,
                )

            if f_lower * f_upper < 0:
                root, info = brentq(
                    problem.objective,
                    self.lower,
                    current_upper,
                    xtol=self.xtol,
                    rtol=self.rtol,
                    maxiter=self.maxiter,
                    full_output=True,
                    disp=False,
                )

                return YieldSolverResult(
                    ytm=float(root),
                    method=YieldSolverMethod.BRENT_EXPANDED,
                    iterations=info.iterations,
                )

            if current_upper == self.max_upper:
                break

            current_upper = min(
                current_upper * self.expansion_factor,
                self.max_upper,
            )

        raise RuntimeError(
            "Expanded Brent root is not bracketed. "
            f"lower={self.lower}, "
            f"initial_upper={self.initial_upper}, "
            f"max_upper={self.max_upper}, "
            f"objective(lower)={f_lower}, "
            f"objective(max_upper)={problem.objective(self.max_upper)}"
        )

def default_yield_solver() -> FallbackYieldSolver:
    return FallbackYieldSolver(
        solvers=[
            NewtonYieldSolver(
                initial_guess=0.10,
                lower=-0.95,
                upper=10.0,
            ),
            BrentYieldSolver(
                lower=-0.95,
                upper=1.50,
            ),
            ExpandedBrentYieldSolver(
                lower=-0.95,
                initial_upper=1.50,
                max_upper=10.0,
                expansion_factor=2.0,
            ),
        ]
    )

def yield_to_maturity(
    problem: YieldProblem,
    solver: YieldSolver | None = None,
    ) -> YieldSolverResult:
    solver = solver or default_yield_solver()
    return solver.solve(problem)

