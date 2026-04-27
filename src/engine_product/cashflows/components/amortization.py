from engine_product.cashflows.components.base import CashflowComponent
from engine_product.cashflows.events import CashflowEvent
from engine_product.cashflows.models import Cashflow, CashflowType
from engine_product.cashflows.state import CashflowState


class AmortizationComponent(CashflowComponent):

    def generate(
        self,
        event: CashflowEvent,
        state: CashflowState,
    ) -> list[Cashflow]:

        if event.amortization_factor <= 0 and event.amortization_amount is None:
            return []

        if event.amortization_amount is not None:
            amount = event.amortization_amount
        else:
            amount = state.outstanding_notional * event.amortization_factor

        notional_after = state.outstanding_notional - amount

        return [
            Cashflow(
                payment_date=event.event_date,
                accrual_start=event.event_date,
                accrual_end=event.event_date,
                amount=amount,
                cashflow_type=CashflowType.AMORTIZATION,
                notional_before=state.outstanding_notional,
                notional_after=notional_after,
                metadata={
                    "amortization_factor": event.amortization_factor,
                },
            )
        ]