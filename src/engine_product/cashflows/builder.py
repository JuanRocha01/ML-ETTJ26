from dataclasses import dataclass, field
from datetime import date

from engine_product.cashflows.components.base import CashflowComponent
from engine_product.cashflows.engine import CashflowEngine
from engine_product.cashflows.events import CashflowEvent
from engine_product.cashflows.models import Cashflow


@dataclass
class CashflowEngineBuilder:
    issue_date: date
    notional: float
    events: list[CashflowEvent]
    components: list[CashflowComponent] = field(default_factory=list)

    def add_component(self, component: CashflowComponent):
        self.components.append(component)
        return self

    def build_engine(self) -> CashflowEngine:
        return CashflowEngine(components=self.components)

    def build_cashflows(self, as_of_date: date) -> list[Cashflow]:
        engine = self.build_engine()

        return engine.build(
            issue_date=self.issue_date,
            events=self.events,
            notional=self.notional,
            as_of_date=as_of_date,
        )