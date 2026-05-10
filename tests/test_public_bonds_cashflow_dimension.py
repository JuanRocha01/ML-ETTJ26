from datetime import date, timedelta

import pandas as pd

from ml_ettj26.pipelines.curve_factory.public_bonds_cashflows.nodes import (
    build_public_bond_cashflow_dimension,
)


def make_calendar_df(start: date = date(2026, 1, 1), end: date = date(2029, 1, 10)):
    rows = []
    current = start
    act_index = 0
    bd_index = 0

    while current <= end:
        is_business_day = current.weekday() < 5

        rows.append(
            {
                "calendar_id": "BR_ANBIMA",
                "date": current,
                "year": current.year,
                "month": current.month,
                "day": current.day,
                "weekday": current.weekday(),
                "is_weekend": current.weekday() >= 5,
                "is_holiday": False,
                "is_business_day": is_business_day,
                "act_index": act_index,
                "bd_index": bd_index,
                "holiday_name": None,
                "source_file_hash": "hash",
            }
        )

        act_index += 1
        if is_business_day:
            bd_index += 1

        current += timedelta(days=1)

    return pd.DataFrame(rows)


def test_public_bond_cashflow_dimension_keeps_engine_cashflow_fields_and_keys():
    instruments = pd.DataFrame(
        [
            {
                "isin": "BRSTNCNTF001",
                "instrument_type": "NTN-F",
                "issue_date": date(2026, 1, 2),
                "maturity_date": date(2027, 1, 1),
            }
        ]
    )

    dimension = build_public_bond_cashflow_dimension(
        instruments=instruments,
        calendar_df=make_calendar_df(),
    )

    assert not dimension.empty
    assert not dimension.duplicated(["isin", "cashflow_number"]).any()

    assert {
        "payment_date",
        "amount",
        "cashflow_type",
        "accrual_start",
        "accrual_end",
        "notional_before",
        "notional_after",
        "metadata_json",
        "payment_bd_index",
        "issue_bd_index",
        "bd_from_issue",
    }.issubset(dimension.columns)

    assert set(dimension["cashflow_type_rank"]) == {10, 30}
    assert (dimension["bd_from_issue"] > 0).all()


def test_public_bond_cashflow_dimension_uses_single_principal_for_ltn():
    instruments = pd.DataFrame(
        [
            {
                "isin": "BRSTNCLTN001",
                "instrument_type": "LTN",
                "issue_date": date(2026, 1, 2),
                "maturity_date": date(2027, 1, 2),
            }
        ]
    )

    dimension = build_public_bond_cashflow_dimension(
        instruments=instruments,
        calendar_df=make_calendar_df(),
    )

    assert len(dimension) == 1
    assert dimension.iloc[0]["cashflow_number"] == 1
    assert dimension.iloc[0]["cashflow_type"] == "PRINCIPAL"
    assert dimension.iloc[0]["cashflow_type_rank"] == 30
    assert dimension.iloc[0]["amount"] == 1000.0
