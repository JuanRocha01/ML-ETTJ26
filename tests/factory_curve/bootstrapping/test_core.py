from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from factory_curve.bootstrapping.core import (
    BootstrapConfig,
    PublicBondBootstrapper,
    bootstrap_public_bond_curves,
)


def _calendar() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2019-12-31", "2020-01-02", "2020-01-03"]
            ),
            "bd_index": [-1, 0, 1],
        }
    )


def _cashflows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"isin": "LTN1", "payment_bd_index": 252, "amount": 1000.0},
            {"isin": "NTNF2", "payment_bd_index": 252, "amount": 50.0},
            {"isin": "NTNF2", "payment_bd_index": 504, "amount": 1050.0},
            {"isin": "NTNF3", "payment_bd_index": 630, "amount": 50.0},
            {"isin": "NTNF3", "payment_bd_index": 756, "amount": 1050.0},
        ]
    )


def _curve_inputs() -> pd.DataFrame:
    d1, d2, d3 = 0.90, 0.80, 0.70
    d_630 = np.exp(0.5 * (np.log(d2) + np.log(d3)))
    return pd.DataFrame(
        [
            {
                "ref_date": "2019-12-31",
                "instrument_type": "LTN",
                "isin": "LTN1",
                "market_pu": 999.0,
            },
            {
                "ref_date": "2020-01-02",
                "instrument_type": "LTN",
                "isin": "LTN1",
                "market_pu": 1000.0 * d1,
            },
            {
                "ref_date": "2020-01-02",
                "instrument_type": "NTN-F",
                "isin": "NTNF2",
                "market_pu": 50.0 * d1 + 1050.0 * d2,
            },
            {
                "ref_date": "2020-01-02",
                "instrument_type": "NTN-F",
                "isin": "NTNF3",
                "market_pu": 50.0 * d_630 + 1050.0 * d3,
            },
        ]
    )


def test_bootstrap_recovers_discount_pillars_and_reprices_coupon_bonds() -> None:
    curves, diagnostics = bootstrap_public_bond_curves(
        curve_inputs=_curve_inputs(),
        cashflow_dimension=_cashflows(),
        calendar_df=_calendar(),
        parameters={
            "start_date": "2020-01-01",
            "max_years": 3,
            "show_progress": False,
        },
    )

    assert curves["ref_date"].unique().tolist() == [
        pd.Timestamp("2020-01-02")
    ]
    by_tenor = curves.set_index("tenor_bd")
    assert by_tenor.loc[252, "discount_factor"] == pytest.approx(0.90)
    assert by_tenor.loc[504, "discount_factor"] == pytest.approx(0.80)
    assert by_tenor.loc[756, "discount_factor"] == pytest.approx(0.70)
    assert by_tenor.loc[[252, 504, 756], "is_bootstrap_pillar"].all()
    assert diagnostics["price_error"].abs().max() < 1.0e-8
    assert set(diagnostics["status"]) == {"BOOTSTRAPPED"}


def test_bootstrapper_builds_static_cashflow_lookup_only_at_initialization() -> None:
    bootstrapper = PublicBondBootstrapper(
        cashflow_dimension=_cashflows(),
        calendar_df=_calendar(),
        config=BootstrapConfig(max_years=3, show_progress=False),
    )
    observations = bootstrapper.prepare_inputs(_curve_inputs())

    result = bootstrapper.bootstrap(
        observations.loc[
            observations["ref_date"].eq(pd.Timestamp("2020-01-02"))
        ]
    )

    assert result.curve["source_instrument_count"].iat[0] == 3
    assert result.curve["bootstrap_pillar_count"].iat[0] == 3


def test_batch_records_date_failure_without_stopping_other_dates() -> None:
    invalid = pd.DataFrame(
        [
            {
                "ref_date": "2020-01-02",
                "instrument_type": "LTN",
                "isin": "MISSING",
                "market_pu": 900.0,
            }
        ]
    )
    curves, diagnostics = bootstrap_public_bond_curves(
        curve_inputs=invalid,
        cashflow_dimension=_cashflows(),
        calendar_df=_calendar(),
        parameters={"show_progress": False},
    )

    assert curves.empty
    assert diagnostics.loc[0, "status"] == "FAILED"
    assert "Cashflow dimension not found" in diagnostics.loc[0, "error_message"]


def test_instruments_with_same_maturity_share_one_fitted_pillar() -> None:
    cashflows = pd.DataFrame(
        [
            {"isin": "A", "payment_bd_index": 252, "amount": 1000.0},
            {"isin": "B", "payment_bd_index": 252, "amount": 2000.0},
        ]
    )
    observations = pd.DataFrame(
        [
            {
                "ref_date": "2020-01-02",
                "instrument_type": "LTN",
                "isin": "A",
                "market_pu": 900.0,
            },
            {
                "ref_date": "2020-01-02",
                "instrument_type": "LTN",
                "isin": "B",
                "market_pu": 1800.0,
            },
        ]
    )

    curves, diagnostics = bootstrap_public_bond_curves(
        curve_inputs=observations,
        cashflow_dimension=cashflows,
        calendar_df=_calendar(),
        parameters={"max_years": 1, "show_progress": False},
    )

    assert curves.set_index("tenor_bd").loc[252, "discount_factor"] == (
        pytest.approx(0.90, abs=1.0e-8)
    )
    assert set(diagnostics["status"]) == {"BOOTSTRAPPED_SHARED_PILLAR"}
    assert diagnostics["price_error"].abs().max() < 1.0e-5
