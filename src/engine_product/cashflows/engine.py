"""
    Engine só sabe:
percorrer eventos
chamar componentes
atualizar estado
"""

from datetime import date

from engine_product.cashflows.components.base import CashflowComponent
from engine_product.cashflows.events import CashflowEvent
from engine_product.cashflows.models import Cashflow, CashflowType
from engine_product.cashflows.state import CashflowState


class CashflowEngine:
    def __init__(self, components: list[CashflowComponent]):
        self.components = components

    def build(
        self,
        issue_date: date,
        events: list[CashflowEvent],
        notional: float,
        as_of_date: date,
    ) -> list[Cashflow]:

        state = CashflowState(
            issue_date=issue_date,
            as_of_date=as_of_date,
            current_period_start=issue_date,
            outstanding_notional=notional,
        )

        cashflows: list[Cashflow] = []

        for event in sorted(events, key=lambda e: e.event_date):
            if event.event_date <= as_of_date:
                self._apply_past_event(event, state)
                continue

            for component in self.components:
                generated = component.generate(event, state)
                cashflows.extend(generated)

            self._apply_event(event, state)

        return sorted(cashflows, key=lambda cf: (cf.payment_date, cf.cashflow_type.value))

    def _apply_past_event(
        self,
        event: CashflowEvent,
        state: CashflowState,
    ) -> None:
        self._apply_event(event, state)

    def _apply_event(
        self,
        event: CashflowEvent,
        state: CashflowState,
    ) -> None:
        if event.amortization_amount is not None:
            state.outstanding_notional -= event.amortization_amount

        elif event.amortization_factor > 0:
            state.outstanding_notional -= (
                state.outstanding_notional * event.amortization_factor
            )

        if event.principal or event.early_redemption:
            state.outstanding_notional = 0.0

        state.current_period_start = event.event_date