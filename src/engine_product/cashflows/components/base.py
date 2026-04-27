from typing import Protocol

from engine_product.cashflows.events import CashflowEvent
from engine_product.cashflows.models import Cashflow
from engine_product.cashflows.state import CashflowState


class CashflowComponent(Protocol):

    def generate(
        self,
        event: CashflowEvent,
        state: CashflowState,
    ) -> list[Cashflow]:
        ...