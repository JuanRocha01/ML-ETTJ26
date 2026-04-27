# engine_product/cashflows/components/optionality.py
# A SER REPENSADO EM COMO ADQUIRIR DADOS DE SPREAD ATRELADO AO RESGATE ANTECIPADO

from engine_product.cashflows.components.base import CashflowComponent
from engine_product.cashflows.events import CashflowEvent
from engine_product.cashflows.models import Cashflow, CashflowType
from engine_product.cashflows.state import CashflowState


class EarlyRedemptionComponent(CashflowComponent):

    def generate(
        self,
        event: CashflowEvent,
        state: CashflowState,
    ) -> list[Cashflow]:

        if not event.early_redemption:
            return []

        redemption_amount = state.outstanding_notional
        premium_amount = state.outstanding_notional * event.redemption_premium

        cashflows = [
            Cashflow(
                payment_date=event.event_date,
                accrual_start=event.event_date,
                accrual_end=event.event_date,
                amount=redemption_amount,
                cashflow_type=CashflowType.EARLY_REDEMPTION,
                notional_before=state.outstanding_notional,
                notional_after=0.0,
            )
        ]

        if premium_amount > 0:
            cashflows.append(
                Cashflow(
                    payment_date=event.event_date,
                    accrual_start=event.event_date,
                    accrual_end=event.event_date,
                    amount=premium_amount,
                    cashflow_type=CashflowType.PREMIUM,
                    notional_before=state.outstanding_notional,
                    notional_after=state.outstanding_notional,
                )
            )

        return cashflows