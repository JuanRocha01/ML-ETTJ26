# engine_product/cashflows/components/principal.py

from engine_product.cashflows.components.base import CashflowComponent
from engine_product.cashflows.events import CashflowEvent
from engine_product.cashflows.models import Cashflow, CashflowType
from engine_product.cashflows.state import CashflowState


class PrincipalComponent(CashflowComponent):

    def generate(
        self,
        event: CashflowEvent,
        state: CashflowState,
    ) -> list[Cashflow]:

        if not event.principal:
            return []

        amount = state.outstanding_notional

        return [
            Cashflow(
                payment_date=event.event_date,
                accrual_start=event.event_date,
                accrual_end=event.event_date,
                amount=amount,
                cashflow_type=CashflowType.PRINCIPAL,
                notional_before=state.outstanding_notional,
                notional_after=0.0,
            )
        ]