from datetime import date

import pytest

from engine_product.cashflows.builder import CashflowEngineBuilder
from engine_product.cashflows.components.interest import InterestComponent
from engine_product.cashflows.components.amortization import AmortizationComponent
from engine_product.cashflows.components.principal import PrincipalComponent
from engine_product.cashflows.components.optionality import EarlyRedemptionComponent
from engine_product.cashflows.events import CashflowEvent
from engine_product.cashflows.indexers.fixed import FixedRateIndexer
from engine_product.cashflows.indexers.cdi import CDIPlusSpreadIndexer
from engine_product.cashflows.indexers.ipca import IPCAPlusSpreadIndexer
from engine_product.cashflows.models import CashflowType


class FakeDayCount:
    def year_fraction(self, start, end):
        return 0.5


class FakeCurve:
    def __init__(self, factor):
        self.factor = factor

    def accumulated_factor(self, start, end):
        return self.factor


def test_simple_fixed_rate_bullet_cashflows():
    events = [
        CashflowEvent(
            event_date=date(2026, 7, 15),
            interest=True,
        ),
        CashflowEvent(
            event_date=date(2027, 1, 15),
            interest=True,
            principal=True,
        ),
    ]

    indexer = FixedRateIndexer(
        annual_rate=0.12,
        day_count=FakeDayCount(),
    )

    cashflows = (
        CashflowEngineBuilder(
            issue_date=date(2026, 1, 15),
            notional=1_000_000,
            events=events,
        )
        .add_component(InterestComponent(indexer=indexer))
        .add_component(PrincipalComponent())
        .build_cashflows(as_of_date=date(2026, 3, 1))
    )

    assert len(cashflows) == 3

    interest_cashflows = [
        cf for cf in cashflows
        if cf.cashflow_type == CashflowType.INTEREST
    ]

    principal_cashflows = [
        cf for cf in cashflows
        if cf.cashflow_type == CashflowType.PRINCIPAL
    ]

    assert interest_cashflows[0].amount == pytest.approx(60_000)
    assert interest_cashflows[1].amount == pytest.approx(60_000)
    assert principal_cashflows[0].amount == pytest.approx(1_000_000)


def test_cdi_plus_spread_amortizing_debenture_cashflows():
    events = [
        CashflowEvent(
            event_date=date(2026, 7, 15),
            interest=True,
        ),
        CashflowEvent(
            event_date=date(2027, 1, 15),
            interest=True,
            amortization_factor=0.30,
        ),
        CashflowEvent(
            event_date=date(2027, 7, 15),
            interest=True,
            amortization_factor=0.30,
        ),
        CashflowEvent(
            event_date=date(2028, 1, 15),
            interest=True,
            principal=True,
        ),
    ]

    indexer = CDIPlusSpreadIndexer(
        cdi_curve=FakeCurve(factor=1.05),
        spread=0.02,
        day_count=FakeDayCount(),
    )

    cashflows = (
        CashflowEngineBuilder(
            issue_date=date(2026, 1, 15),
            notional=1_000_000,
            events=events,
        )
        .add_component(InterestComponent(indexer=indexer))
        .add_component(AmortizationComponent())
        .add_component(PrincipalComponent())
        .build_cashflows(as_of_date=date(2026, 3, 1))
    )

    interest = [
        cf for cf in cashflows
        if cf.cashflow_type == CashflowType.INTEREST
    ]

    amortization = [
        cf for cf in cashflows
        if cf.cashflow_type == CashflowType.AMORTIZATION
    ]

    principal = [
        cf for cf in cashflows
        if cf.cashflow_type == CashflowType.PRINCIPAL
    ]

    assert len(interest) == 4
    assert len(amortization) == 2
    assert len(principal) == 1

    assert interest[0].amount == pytest.approx(60_000)
    assert interest[1].amount == pytest.approx(60_000)

    assert amortization[0].amount == pytest.approx(300_000)

    assert interest[2].notional_before == pytest.approx(700_000)
    assert interest[2].amount == pytest.approx(42_000)

    assert amortization[1].amount == pytest.approx(210_000)

    assert principal[0].amount == pytest.approx(490_000)


def test_ipca_plus_spread_cra_with_early_redemption():
    events = [
        CashflowEvent(
            event_date=date(2026, 7, 15),
            interest=True,
        ),
        CashflowEvent(
            event_date=date(2027, 1, 15),
            interest=True,
            amortization_factor=0.20,
        ),
        CashflowEvent(
            event_date=date(2027, 7, 15),
            interest=True,
            early_redemption=True,
            redemption_premium=0.01,
        ),
    ]

    indexer = IPCAPlusSpreadIndexer(
        inflation_curve=FakeCurve(factor=1.03),
        spread=0.06,
        day_count=FakeDayCount(),
    )

    cashflows = (
        CashflowEngineBuilder(
            issue_date=date(2026, 1, 15),
            notional=1_000_000,
            events=events,
        )
        .add_component(InterestComponent(indexer=indexer))
        .add_component(AmortizationComponent())
        .add_component(EarlyRedemptionComponent())
        .build_cashflows(as_of_date=date(2026, 2, 1))
    )

    interest = [
        cf for cf in cashflows
        if cf.cashflow_type == CashflowType.INTEREST
    ]

    amortization = [
        cf for cf in cashflows
        if cf.cashflow_type == CashflowType.AMORTIZATION
    ]

    early_redemption = [
        cf for cf in cashflows
        if cf.cashflow_type == CashflowType.EARLY_REDEMPTION
    ]

    premium = [
        cf for cf in cashflows
        if cf.cashflow_type == CashflowType.PREMIUM
    ]

    assert len(interest) == 3
    assert len(amortization) == 1
    assert len(early_redemption) == 1
    assert len(premium) == 1

    assert interest[0].amount == pytest.approx(60_000)
    assert interest[1].amount == pytest.approx(60_000)

    assert amortization[0].amount == pytest.approx(200_000)

    assert interest[2].notional_before == pytest.approx(800_000)
    assert interest[2].amount == pytest.approx(48_000)

    assert early_redemption[0].amount == pytest.approx(800_000)
    assert premium[0].amount == pytest.approx(8_000)