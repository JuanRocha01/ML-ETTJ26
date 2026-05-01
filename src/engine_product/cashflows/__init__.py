"""
    models.py      → objetos finais: Cashflow
    events.py      → eventos contratuais
    state.py       → estado do contrato: saldo devedor, accrual, etc.
    components/    → peças que criam fluxos
    indexers/      → regras de remuneração
    builder.py     → monta instrumentos via composição
    engine.py      → executa os componentes

Public API for cashflow generation.

This module exposes the core cashflow models, events and builders used by
instrument contracts to generate projected cashflows.
"""

from engine_product.cashflows.builder import CashflowEngineBuilder
from engine_product.cashflows.events import CashflowEvent
from engine_product.cashflows.models import Cashflow, CashflowType

__all__ = [
    "Cashflow",
    "CashflowType",
    "CashflowEvent",
    "CashflowEngineBuilder",
]