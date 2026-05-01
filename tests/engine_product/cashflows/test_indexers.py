from datetime import date

import pytest

from engine_product.cashflows.indexers.fixed import CompoundFixedRateIndexer
from engine_product.cashflows.indexers.cdi import CDIPlusSpreadIndexer
from engine_product.cashflows.indexers.ipca import IPCAPlusSpreadIndexer


class FakeDayCount:
    def year_fraction(self, start, end):
        return 0.5


class FakeCurve:
    def __init__(self, factor):
        self.factor = factor

    def accumulated_factor(self, start, end):
        return self.factor


def test_compound_fixed_rate_indexer_returns_rate_times_year_fraction():
    indexer = CompoundFixedRateIndexer(
        annual_rate=0.12,
        day_count=FakeDayCount(),
    )

    result = indexer.accrual_factor(
        date(2026, 1, 1),
        date(2026, 7, 1),
    )

    assert result == pytest.approx(0.0583005244)


def test_cdi_plus_spread_indexer():
    indexer = CDIPlusSpreadIndexer(
        cdi_curve=FakeCurve(factor=1.05),
        spread=0.02,
        day_count=FakeDayCount(),
    )

    result = indexer.accrual_factor(
        date(2026, 1, 1),
        date(2026, 7, 1),
    )

    assert result == pytest.approx(0.06)


def test_ipca_plus_spread_indexer():
    indexer = IPCAPlusSpreadIndexer(
        inflation_curve=FakeCurve(factor=1.03),
        spread=0.06,
        day_count=FakeDayCount(),
    )

    result = indexer.accrual_factor(
        date(2026, 1, 1),
        date(2026, 7, 1),
    )

    assert result == pytest.approx(0.06)