import pandas as pd
import pytest

from engine_product.pricing.cashflow_arrays import (
    build_cashflow_schedule_lookup,
    macaulay_duration_from_time_amount_pairs,
    price_from_time_amount_pairs,
)


def test_cashflow_schedule_lookup_aggregates_amounts_by_payment_bd_index():
    dimension = pd.DataFrame(
        [
            {"isin": "ABC", "payment_bd_index": 252, "amount": 50.0},
            {"isin": "ABC", "payment_bd_index": 252, "amount": 100.0},
            {"isin": "ABC", "payment_bd_index": 504, "amount": 1000.0},
        ]
    )

    lookup = build_cashflow_schedule_lookup(dimension)
    pairs = lookup["ABC"].time_amount_pairs_as_of(ref_bd_index=0)

    assert pairs == ((1.0, 150.0), (2.0, 1000.0))


def test_cashflow_schedule_lookup_filters_cashflows_by_as_of_bd_index():
    dimension = pd.DataFrame(
        [
            {"isin": "ABC", "payment_bd_index": 252, "amount": 150.0},
            {"isin": "ABC", "payment_bd_index": 504, "amount": 1000.0},
        ]
    )

    lookup = build_cashflow_schedule_lookup(dimension)
    pairs = lookup["ABC"].time_amount_pairs_as_of(ref_bd_index=300)

    assert pairs == ((204 / 252.0, 1000.0),)

    tenor_bd, amounts = lookup["ABC"].future_arrays_as_of(ref_bd_index=300)
    assert tenor_bd.tolist() == [204]
    assert amounts.tolist() == [1000.0]


def test_price_and_duration_from_time_amount_pairs():
    pairs = ((1.0, 50.0), (2.0, 1050.0))
    price = 50.0 / (1.10**1) + 1050.0 / (1.10**2)

    assert price_from_time_amount_pairs(pairs, 0.10) == pytest.approx(price)
    assert macaulay_duration_from_time_amount_pairs(pairs, 0.10) > 0.0
