from dataclasses import dataclass
from datetime import date


@dataclass
class CashflowState:
    issue_date: date
    as_of_date: date
    current_period_start: date
    outstanding_notional: float