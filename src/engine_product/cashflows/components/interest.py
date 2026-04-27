from engine_product.cashflows.components.base import CashflowComponent
from engine_product.cashflows.events import CashflowEvent
from engine_product.cashflows.models import Cashflow, CashflowType
from engine_product.cashflows.state import CashflowState


class InterestComponent(CashflowComponent):
    def __init__(self, indexer):
        self.indexer = indexer

    def generate(
        self,
        event: CashflowEvent,
        state: CashflowState,
    ) -> list[Cashflow]:

        if not event.interest:
            return []

        accrual_factor = self.indexer.accrual_factor(
            state.current_period_start,
            event.event_date,
        )

        amount = state.outstanding_notional * accrual_factor

        return [
            Cashflow(
                payment_date=event.event_date,
                accrual_start=state.current_period_start,
                accrual_end=event.event_date,
                amount=amount,
                cashflow_type=CashflowType.INTEREST,
                notional_before=state.outstanding_notional,
                notional_after=state.outstanding_notional,
                metadata={
                    "accrual_factor": accrual_factor,
                },
            )
        ]