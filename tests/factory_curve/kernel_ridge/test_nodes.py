from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from factory_curve.kernel_ridge.nodes import (
    build_kernel_ridge_curve_batches,
    build_kernel_ridge_model_dimension,
    fit_kernel_ridge_models,
    select_kernel_ridge_calibration_dates,
    tune_kernel_ridge_hyperparameters,
)


REFERENCE_DATES = {
    pd.Timestamp("2019-01-02"): 0,
    pd.Timestamp("2019-02-01"): 20,
    pd.Timestamp("2020-01-02"): 252,
}
PAYMENT_INDICES = (500, 750, 1000, 1250, 1500)


def make_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    curve_rows = []
    for ref_date, ref_bd_index in REFERENCE_DATES.items():
        for index, payment_bd_index in enumerate(PAYMENT_INDICES):
            tenor_bd = payment_bd_index - ref_bd_index
            tenor_years = tenor_bd / 252.0
            market_rate = 0.10
            curve_rows.append(
                {
                    "ref_date": ref_date,
                    "instrument_type": "LTN",
                    "isin": f"BOND{index}",
                    "bd_to_maturity": tenor_bd,
                    "market_pu": 1000.0
                    / np.power(1.0 + market_rate, tenor_years),
                    "modified_duration": tenor_years
                    / (1.0 + market_rate),
                    "flag_volume": "HIGH",
                    "flag_cobertura_tenors": "GOOD",
                    "flag_ocupacao_tenors": "GOOD",
                }
            )
    cashflows = pd.DataFrame(
        {
            "isin": [f"BOND{index}" for index in range(5)],
            "payment_bd_index": PAYMENT_INDICES,
            "amount": [1000.0] * 5,
        }
    )
    calendar = pd.DataFrame(
        {
            "date": list(REFERENCE_DATES),
            "bd_index": list(REFERENCE_DATES.values()),
        }
    )
    return pd.DataFrame(curve_rows), cashflows, calendar


def make_parameters() -> dict:
    return {
        "tuning_cutoff_date": "2020-01-01",
        "production_start_date": "2020-01-01",
        "min_maturity_bd": 90,
        "min_observations": 4,
        "business_days_per_year": 252,
        "max_years": 1,
        "model_batch_size": 2,
        "show_progress": False,
        "condition_number_limit": 1.0e16,
        "hyperparameter_grid": {
            "alpha": [0.05],
            "delta": [0.0],
            "ridge": [1.0],
        },
    }


def test_calibration_selection_is_monthly_and_strictly_pre_2020() -> None:
    curve_inputs, _, _ = make_inputs()

    result = select_kernel_ridge_calibration_dates(
        curve_inputs,
        make_parameters(),
    )

    assert result["ref_date"].dt.strftime("%Y-%m-%d").tolist() == [
        "2019-01-02",
        "2019-02-01",
    ]
    assert result["ref_date"].max() < pd.Timestamp("2020-01-01")


def test_tuning_and_production_nodes_freeze_pre_2020_parameters() -> None:
    curve_inputs, cashflows, calendar = make_inputs()
    parameters = make_parameters()
    calibration_dates = select_kernel_ridge_calibration_dates(
        curve_inputs,
        parameters,
    )

    search, selected = tune_kernel_ridge_hyperparameters(
        curve_inputs,
        cashflows,
        calendar,
        calibration_dates,
        parameters,
    )
    models = fit_kernel_ridge_models(
        curve_inputs,
        cashflows,
        calendar,
        selected,
        parameters,
    )
    dimension = build_kernel_ridge_model_dimension(models)
    curve_batches = build_kernel_ridge_curve_batches(models, parameters)
    curve = next(iter(curve_batches.values()))()

    assert len(search) == 1
    assert selected.iloc[0]["is_best"]
    assert selected.iloc[0]["tuning_last_date"] < pd.Timestamp("2020-01-01")
    assert set(models) == {"2020-01-02"}
    assert dimension["ref_date"].dt.strftime("%Y-%m-%d").tolist() == [
        "2020-01-02"
    ]
    assert curve["ref_date"].dt.strftime("%Y-%m-%d").unique().tolist() == [
        "2020-01-02"
    ]
    assert len(curve) == 252


def test_tuning_rejects_any_calibration_date_from_2020_onward() -> None:
    curve_inputs, cashflows, calendar = make_inputs()
    invalid_dates = pd.DataFrame(
        {
            "ref_date": [pd.Timestamp("2020-01-02")],
            "tuning_cutoff_date": [pd.Timestamp("2020-01-01")],
        }
    )

    with pytest.raises(ValueError, match="Data leakage guard"):
        tune_kernel_ridge_hyperparameters(
            curve_inputs,
            cashflows,
            calendar,
            invalid_dates,
            make_parameters(),
        )
