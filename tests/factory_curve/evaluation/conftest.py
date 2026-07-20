from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def evaluation_sample():
    dates = pd.bdate_range("2020-01-02", periods=10)
    tenors = np.arange(1, 21)
    curve = pd.DataFrame(
        [
            0.05 + 0.0001 * tenors + date_index * 0.0002
            for date_index in range(len(dates))
        ],
        index=dates,
        columns=[str(tenor) for tenor in tenors],
    )
    curve.index.name = "ref_date"

    ltn_rows = []
    for instrument_index, tenor in enumerate((5, 8, 10, 15)):
        for date_index in (0, 1):
            remaining_tenor = tenor - date_index
            rate = (
                0.05
                + 0.0001 * remaining_tenor
                + date_index * 0.0002
            )
            ltn_rows.append(
                {
                    "ref_date": dates[date_index],
                    "instrument_type": "LTN",
                    "isin": f"LTN{instrument_index}",
                    "bd_to_maturity": remaining_tenor,
                    "market_ytm": rate,
                    "market_pu": (
                        1000.0
                        / (1.0 + rate) ** (remaining_tenor / 252.0)
                    ),
                }
            )

    swaps = pd.DataFrame(
        [
            {
                "date": dates[0],
                "maturity": dates[5],
                "product_code": "T1PRE",
                "bd_to_maturity": 5,
                "adjusted_value": 5.05,
            }
        ]
    )
    calendar = pd.DataFrame(
        {
            "date": dates,
            "is_business_day": True,
            "bd_index": np.arange(len(dates)),
        }
    )
    parameters = {
        "swap_rate_scale": 0.01,
        "notional": 1000.0,
        "business_days_per_year": 252,
        "pca_tenor_step_bd": 1,
        "minimum_forward_rate": 0.0,
        "maximum_forward_rate": 1.0,
        "rolldown_start_date": "2020-01-01",
        "rolldown_end_date": "2020-12-31",
        "rolldown_short_end_bd": 6,
        "rolldown_medium_end_bd": 12,
        "rolldown_short_targets_bd": [5],
        "rolldown_medium_targets_bd": [8, 10],
        "rolldown_long_targets_bd": [15],
        "curve_derivative_step_bd": 1,
        "price_sensitivity_bump": 0.0001,
    }
    return {
        "curve": curve,
        "ltn": pd.DataFrame(ltn_rows),
        "swaps": swaps,
        "calendar": calendar,
        "parameters": parameters,
    }
