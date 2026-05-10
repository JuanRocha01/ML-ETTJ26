from datetime import date, timedelta

import pandas as pd
import pytest

from engine_product.calendars.business_calendar import BusinessCalendar
from engine_product.calendars.repository import DataFrameCalendarRepository
from engine_product.convention.conventions import BU252
from engine_product.instruments.public_bonds import NTNFContract
from ml_ettj26.pipelines.curve_factory.public_bonds_mart.nodes_batch import (
    build_public_bonds_curve_inputs_batch,
)


def make_calendar_df(start: date = date(2026, 1, 1), end: date = date(2028, 1, 10)):
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


def price_from_yield(cashflows, ytm, settlement_date, day_count):
    price = 0.0

    for cashflow in cashflows:
        if cashflow.payment_date <= settlement_date:
            continue

        t = day_count.year_fraction(settlement_date, cashflow.payment_date)
        price += cashflow.amount / ((1.0 + ytm) ** t)

    return price


def make_curve_candidate(
    *,
    instrument_type,
    isin,
    ref_date,
    issue_date,
    maturity_date,
    pu_med,
    taxa_med=None,
    primary_quote_type="PRICE",
):
    return {
        "ref_date": ref_date,
        "instrument_type": instrument_type,
        "isin": isin,
        "issue_date": issue_date,
        "maturity_date": maturity_date,
        "bd_to_maturity": 252,
        "pu_med": pu_med,
        "taxa_med": taxa_med,
        "quote_quality": "OK",
        "quote_source": "TEST",
        "primary_quote_type": primary_quote_type,
    }


def test_batch_node_solves_price_quotes_with_batch_solver_methods():
    calendar_df = make_calendar_df()
    calendar = BusinessCalendar(DataFrameCalendarRepository(calendar_df))
    day_count = BU252(calendar)

    ref_date = date(2026, 1, 2)
    issue_date = date(2026, 1, 2)

    ltn_maturity = date(2027, 1, 4)
    ltn_t = day_count.year_fraction(ref_date, ltn_maturity)
    ltn_price = 1000.0 / ((1.0 + 0.10) ** ltn_t)

    ntnf_maturity = date(2027, 1, 1)
    ntnf = NTNFContract(
        start_date=issue_date,
        maturity_date=ntnf_maturity,
        calendar=calendar,
        day_count=day_count,
    )
    ntnf_cashflows = ntnf.build_cashflows(as_of_date=ref_date)
    ntnf_price = price_from_yield(
        ntnf_cashflows,
        ytm=0.10,
        settlement_date=ref_date,
        day_count=day_count,
    )

    curve_candidates = pd.DataFrame(
        [
            make_curve_candidate(
                instrument_type="LTN",
                isin="BRSTNCLTN001",
                ref_date=ref_date,
                issue_date=issue_date,
                maturity_date=ltn_maturity,
                pu_med=ltn_price,
            ),
            make_curve_candidate(
                instrument_type="NTN-F",
                isin="BRSTNCNTF001",
                ref_date=ref_date,
                issue_date=issue_date,
                maturity_date=ntnf_maturity,
                pu_med=ntnf_price,
            ),
        ]
    )

    inputs, failures = build_public_bonds_curve_inputs_batch(
        curve_candidates=curve_candidates,
        calendar_df=calendar_df,
    )

    assert failures.empty
    assert len(inputs) == 2

    by_type = inputs.set_index("instrument_type")

    assert by_type.loc["LTN", "solver_method"] == "ZERO_COUPON"
    assert by_type.loc["LTN", "market_ytm"] == pytest.approx(0.10, abs=1e-10)

    assert by_type.loc["NTN-F", "solver_method"] == "NEWTON_BATCH"
    assert by_type.loc["NTN-F", "market_ytm"] == pytest.approx(0.10, abs=1e-10)


def test_batch_node_returns_individual_failures_without_dropping_successes():
    calendar_df = make_calendar_df()
    ref_date = date(2026, 1, 2)
    issue_date = date(2026, 1, 2)

    curve_candidates = pd.DataFrame(
        [
            make_curve_candidate(
                instrument_type="LTN",
                isin="BRSTNCLTN001",
                ref_date=ref_date,
                issue_date=issue_date,
                maturity_date=date(2027, 1, 4),
                pu_med=900.0,
            ),
            make_curve_candidate(
                instrument_type="LTN",
                isin="BRSTNCLTN_BAD",
                ref_date=ref_date,
                issue_date=issue_date,
                maturity_date=date(2027, 1, 4),
                pu_med=6000.0,
            ),
        ]
    )

    inputs, failures = build_public_bonds_curve_inputs_batch(
        curve_candidates=curve_candidates,
        calendar_df=calendar_df,
    )

    assert len(inputs) == 1
    assert inputs.iloc[0]["isin"] == "BRSTNCLTN001"
    assert inputs.iloc[0]["solver_method"] == "ZERO_COUPON"

    assert len(failures) == 1
    assert failures.iloc[0]["isin"] == "BRSTNCLTN_BAD"
    assert failures.iloc[0]["calculation_error_type"] == "InvalidPrice"
