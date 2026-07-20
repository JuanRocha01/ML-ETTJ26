from __future__ import annotations

import numpy as np
import pytest

from factory_curve.kernel_ridge.core import (
    DailyCurveData,
    KernelRidgeConfig,
    fit_kernel_ridge_model,
    loocv_yield_error_squares,
)
from factory_curve.kernel_ridge.model import kernel_matrix


def test_delta_zero_kernel_matches_reference_formula() -> None:
    alpha = 0.05
    x = np.array([1.0, 2.0])
    y = np.array([0.5, 3.0])

    result = kernel_matrix(x, y, alpha=alpha, delta=0.0)

    minimum = np.minimum(x[:, None], y[None, :])
    maximum = np.maximum(x[:, None], y[None, :])
    expected = (
        -minimum / alpha**2 * np.exp(-alpha * minimum)
        + 2.0 / alpha**3 * (1.0 - np.exp(-alpha * minimum))
        - minimum / alpha**2 * np.exp(-alpha * maximum)
    )
    np.testing.assert_allclose(result, expected)
    np.testing.assert_allclose(
        kernel_matrix(x, x, alpha=alpha, delta=0.0),
        kernel_matrix(x, x, alpha=alpha, delta=0.0).T,
    )


def test_daily_model_produces_discount_and_effective_rate_curve() -> None:
    tenors = np.array([252, 504, 756, 1008], dtype=np.int64)
    cashflows = np.eye(4) * 1000.0
    flat_rate = 0.10
    years = tenors / 252.0
    prices = 1000.0 / np.power(1.0 + flat_rate, years)
    data = DailyCurveData(
        reference_date="2020-01-02",
        isins=("A", "B", "C", "D"),
        prices=prices,
        modified_durations=years / (1.0 + flat_rate),
        cashflow_tenors_bd=tenors,
        cashflow_matrix=cashflows,
    )
    config = KernelRidgeConfig(
        alpha_values=(0.05,),
        delta_values=(0.0,),
        ridge_values=(0.01,),
        show_progress=False,
    )

    model = fit_kernel_ridge_model(
        data,
        alpha=0.05,
        delta=0.0,
        ridge=0.01,
        config=config,
    )
    curve = model.curve_frame(max_years=1)

    assert len(curve) == 252
    assert curve["is_valid_discount_factor"].all()
    assert curve.iloc[-1]["fitted_rate"] == pytest.approx(
        flat_rate,
        abs=2.0e-3,
    )
    assert model.n_observations == 4


def test_press_loocv_matches_explicit_leave_one_security_out() -> None:
    tenors = np.array([252, 504, 756, 1008], dtype=np.int64)
    years = tenors / 252.0
    cashflows = np.eye(4) * 1000.0
    prices = 1000.0 / np.power(1.10, years)
    durations = years / 1.10
    data = DailyCurveData(
        reference_date="2019-01-02",
        isins=("A", "B", "C", "D"),
        prices=prices,
        modified_durations=durations,
        cashflow_tenors_bd=tenors,
        cashflow_matrix=cashflows,
    )
    config = KernelRidgeConfig(
        alpha_values=(0.05,),
        delta_values=(0.0,),
        ridge_values=(1.0,),
        show_progress=False,
        condition_number_limit=1.0e16,
    )
    alpha, delta, ridge = 0.05, 0.0, 1.0

    press_errors = loocv_yield_error_squares(
        data,
        alpha=alpha,
        delta=delta,
        ridge=ridge,
        config=config,
    )

    kernel = kernel_matrix(
        years,
        years,
        alpha=alpha,
        delta=delta,
    )
    explicit_errors = []
    ridge_scaled = ridge / tenors[-1]
    for held_out in range(data.n_observations):
        train = np.arange(data.n_observations) != held_out
        train_cashflows = cashflows[train]
        train_prices = prices[train]
        gram = train_cashflows @ kernel @ train_cashflows.T
        inverse_weights = (
            np.square(durations[train] * train_prices)
            * (data.n_observations - 1)
        )
        system = gram + np.diag(ridge_scaled * inverse_weights)
        dual = np.linalg.solve(
            system,
            train_prices - train_cashflows.sum(axis=1),
        )
        coefficients = train_cashflows.T @ dual
        held_out_price = float(
            cashflows[held_out] @ (1.0 + kernel @ coefficients)
        )
        yield_error = (prices[held_out] - held_out_price) / (
            durations[held_out] * prices[held_out]
        )
        explicit_errors.append(yield_error**2)

    np.testing.assert_allclose(
        press_errors,
        explicit_errors,
        rtol=1.0e-8,
        atol=1.0e-14,
    )
