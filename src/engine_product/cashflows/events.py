from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class CashflowEvent:
    event_date: date

    interest: bool = True

    amortization_factor: float = 0.0
    amortization_amount: float | None = None

    principal: bool = False

    early_redemption: bool = False
    redemption_premium: float = 0.0

    metadata: dict | None = None