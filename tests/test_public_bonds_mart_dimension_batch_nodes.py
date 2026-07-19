from datetime import date, timedelta

import pandas as pd
import pytest

from ml_ettj26.pipelines.curve_factory.public_bonds_mart.nodes_dimension_batch import (
    build_public_bonds_curve_inputs_from_cashflow_dimension,
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


def bd_index(calendar_df: pd.DataFrame, d: date) -> int:
    return int(calendar_df.loc[calendar_df["date"] == d, "bd_index"].iloc[0])


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
        "numero_observacoes_dia": 10,
        "numero_observacoes_curto": 2,
        "numero_observacoes_medio": 4,
        "numero_observacoes_longo": 4,
        "flag_volume": "MEDIUM",
        "flag_cobertura_tenors": "GOOD",
        "flag_ocupacao_tenors": "GOOD",
    }


def test_dimension_batch_node_uses_cashflow_dimension_for_batch_yields():
    calendar_df = make_calendar_df()
    ref_date = date(2026, 1, 2)
    issue_date = date(2026, 1, 2)
    one_year_payment = date(2027, 1, 4)
    two_year_payment = date(2028, 1, 3)

    ref_bd = bd_index(calendar_df, ref_date)
    one_year_bd = bd_index(calendar_df, one_year_payment)
    two_year_bd = bd_index(calendar_df, two_year_payment)

    ltn_t = (one_year_bd - ref_bd) / 252.0
    ltn_price = 1000.0 / ((1.0 + 0.10) ** ltn_t)

    ntnf_pairs = (
        ((one_year_bd - ref_bd) / 252.0, 50.0),
        ((two_year_bd - ref_bd) / 252.0, 1050.0),
    )
    ntnf_price = sum(amount / ((1.0 + 0.10) ** t) for t, amount in ntnf_pairs)

    curve_candidates = pd.DataFrame(
        [
            make_curve_candidate(
                instrument_type="LTN",
                isin="LTN1",
                ref_date=ref_date,
                issue_date=issue_date,
                maturity_date=one_year_payment,
                pu_med=ltn_price,
            ),
            make_curve_candidate(
                instrument_type="NTN-F",
                isin="NTNF1",
                ref_date=ref_date,
                issue_date=issue_date,
                maturity_date=two_year_payment,
                pu_med=ntnf_price,
            ),
        ]
    )

    cashflow_dimension = pd.DataFrame(
        [
            {
                "isin": "LTN1",
                "payment_bd_index": one_year_bd,
                "amount": 1000.0,
            },
            {
                "isin": "NTNF1",
                "payment_bd_index": one_year_bd,
                "amount": 50.0,
            },
            {
                "isin": "NTNF1",
                "payment_bd_index": two_year_bd,
                "amount": 50.0,
            },
            {
                "isin": "NTNF1",
                "payment_bd_index": two_year_bd,
                "amount": 1000.0,
            },
        ]
    )

    inputs, failures = build_public_bonds_curve_inputs_from_cashflow_dimension(
        curve_candidates=curve_candidates,
        cashflow_dimension=cashflow_dimension,
        calendar_df=calendar_df,
    )

    assert failures.empty
    assert len(inputs) == 2

    by_isin = inputs.set_index("isin")

    assert by_isin.loc["LTN1", "solver_method"] == "ZERO_COUPON"
    assert by_isin.loc["LTN1", "market_ytm"] == pytest.approx(0.10, abs=1e-10)
    assert by_isin.loc["LTN1", "numero_observacoes_curto"] == 2
    assert by_isin.loc["LTN1", "numero_observacoes_medio"] == 4
    assert by_isin.loc["LTN1", "numero_observacoes_longo"] == 4
    assert by_isin.loc["LTN1", "flag_ocupacao_tenors"] == "GOOD"

    assert by_isin.loc["NTNF1", "solver_method"] == "NEWTON_BATCH"
    assert by_isin.loc["NTNF1", "market_ytm"] == pytest.approx(0.10, abs=1e-10)


def test_dimension_batch_node_returns_failure_when_isin_is_missing_from_dimension():
    calendar_df = make_calendar_df()
    ref_date = date(2026, 1, 2)

    curve_candidates = pd.DataFrame(
        [
            make_curve_candidate(
                instrument_type="LTN",
                isin="MISSING",
                ref_date=ref_date,
                issue_date=ref_date,
                maturity_date=date(2027, 1, 4),
                pu_med=900.0,
            )
        ]
    )

    inputs, failures = build_public_bonds_curve_inputs_from_cashflow_dimension(
        curve_candidates=curve_candidates,
        cashflow_dimension=pd.DataFrame(columns=["isin", "payment_bd_index", "amount"]),
        calendar_df=calendar_df,
    )

    assert inputs.empty
    assert len(failures) == 1
    assert failures.iloc[0]["calculation_error_type"] == "ValueError"
    assert "Cashflow dimension not found" in failures.iloc[0]["calculation_error_message"]
