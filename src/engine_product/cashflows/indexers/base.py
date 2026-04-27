from datetime import date
from typing import Protocol


class Indexer(Protocol):

    def accrual_factor(
        self,
        start: date,
        end: date,
    ) -> float:
        ...