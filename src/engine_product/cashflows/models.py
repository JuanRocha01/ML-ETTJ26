from dataclasses import dataclass
from datetime import date
from enum import Enum


class CashflowType(str, Enum):
    INTEREST = "INTEREST"
    AMORTIZATION = "AMORTIZATION"
    PRINCIPAL = "PRINCIPAL"
    EARLY_REDEMPTION = "EARLY_REDEMPTION"
    PREMIUM = "PREMIUM"
    FEE = "FEE"


@dataclass(frozen=True)
class Cashflow:
    payment_date: date
    amount: float
    cashflow_type: CashflowType

    accrual_start: date | None = None
    accrual_end: date | None = None

    notional_before: float | None = None
    notional_after: float | None = None

    metadata: dict | None = None
