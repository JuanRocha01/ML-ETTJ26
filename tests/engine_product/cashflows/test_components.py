from datetime import date

import pytest

from engine_product.cashflows.components.interest import InterestComponent
from engine_product.cashflows.components.amortization import AmortizationComponent
from engine_product.cashflows.components.principal import PrincipalComponent
from engine_product.cashflows.components.optionality import EarlyRedemptionComponent
from engine_product.cashflows.events import CashflowEvent
from engine_product.cashflows.models import CashflowType
from engine_product.cashflows.state import CashflowState


class FakeIndexer:
    def accrual_factor(self, start, end):
        return 0.06


def make_state():
    return CashflowState(
        issue_date=date(2026, 1, 15),
        as_of_date=date(2026, 3, 1),
        current_period_start=date(2026, 1, 15),
        outstanding_notional=1_000_000,
    )


def test_interest_component_generates_interest_cashflow():
    component = InterestComponent(indexer=FakeIndexer())

    event = CashflowEvent(
        event_date=date(2026, 7, 15),
        interest=True,
    )

    result = component.generate(event, make_state())

    assert len(result) == 1
    assert result[0].cashflow_type == CashflowType.INTEREST
    assert result[0].amount == pytest.approx(60_000)
    assert result[0].notional_before == 1_000_000
    assert result[0].notional_after == 1_000_000


def test_interest_component_returns_empty_when_event_has_no_interest():
    component = InterestComponent(indexer=FakeIndexer())

    event = CashflowEvent(
        event_date=date(2026, 7, 15),
        interest=False,
    )

    result = component.generate(event, make_state())

    assert result == []


def test_amortization_component_generates_factor_amortization():
    component = AmortizationComponent()

    event = CashflowEvent(
        event_date=date(2027, 1, 15),
        interest=True,
        amortization_factor=0.30,
    )

    result = component.generate(event, make_state())

    assert len(result) == 1
    assert result[0].cashflow_type == CashflowType.AMORTIZATION
    assert result[0].amount == pytest.approx(300_000)
    assert result[0].notional_before == 1_000_000
    assert result[0].notional_after == 700_000


def test_amortization_component_generates_fixed_amount_amortization():
    component = AmortizationComponent()

    event = CashflowEvent(
        event_date=date(2027, 1, 15),
        amortization_amount=250_000,
    )

    result = component.generate(event, make_state())

    assert result[0].amount == pytest.approx(250_000)
    assert result[0].notional_after == pytest.approx(750_000)


def test_principal_component_generates_remaining_principal():
    component = PrincipalComponent()

    event = CashflowEvent(
        event_date=date(2028, 1, 15),
        principal=True,
    )

    result = component.generate(event, make_state())

    assert len(result) == 1
    assert result[0].cashflow_type == CashflowType.PRINCIPAL
    assert result[0].amount == pytest.approx(1_000_000)
    assert result[0].notional_after == 0.0


def test_early_redemption_component_generates_redemption_and_premium():
    component = EarlyRedemptionComponent()

    event = CashflowEvent(
        event_date=date(2027, 7, 15),
        early_redemption=True,
        redemption_premium=0.01,
    )

    result = component.generate(event, make_state())

    assert len(result) == 2

    assert result[0].cashflow_type == CashflowType.EARLY_REDEMPTION
    assert result[0].amount == pytest.approx(1_000_000)

    assert result[1].cashflow_type == CashflowType.PREMIUM
    assert result[1].amount == pytest.approx(10_000)