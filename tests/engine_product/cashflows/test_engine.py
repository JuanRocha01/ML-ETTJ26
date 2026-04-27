from datetime import date

import pytest

from engine_product.cashflows.components.interest import InterestComponent
from engine_product.cashflows.components.amortization import AmortizationComponent
from engine_product.cashflows.components.principal import PrincipalComponent
from engine_product.cashflows.engine import CashflowEngine
from engine_product.cashflows.events import CashflowEvent
from engine_product.cashflows.models import CashflowType


class FakeIndexer:
    def accrual_factor(self, start, end):
        return 0.10


def test_engine_skips_past_events_but_updates_outstanding_notional():
    events = [
        CashflowEvent(
            event_date=date(2026, 1, 15),
            interest=True,
            amortization_factor=0.20,
        ),
        CashflowEvent(
            event_date=date(2026, 7, 15),
            interest=True,
            principal=True,
        ),
    ]

    engine = CashflowEngine(
        components=[
            InterestComponent(FakeIndexer()),
            AmortizationComponent(),
            PrincipalComponent(),
        ]
    )

    result = engine.build(
        issue_date=date(2025, 7, 15),
        events=events,
        notional=1_000_000,
        as_of_date=date(2026, 3, 1),
    )

    interest = [cf for cf in result if cf.cashflow_type == CashflowType.INTEREST][0]
    principal = [cf for cf in result if cf.cashflow_type == CashflowType.PRINCIPAL][0]

    assert interest.amount == pytest.approx(80_000)
    assert interest.notional_before == pytest.approx(800_000)

    assert principal.amount == pytest.approx(800_000)