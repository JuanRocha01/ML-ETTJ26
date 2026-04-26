from abc import ABC, abstractmethod
from datetime import date

class BusinessCalendarRepository(ABC):

    @abstractmethod
    def get(self, d: date) -> dict:
        """Retorna a linha completa de uma data."""
        ...

    @abstractmethod
    def actual_days_between(self, start: date, end: date) -> int:
        """Diferença de act_index entre duas datas."""
        ...

    @abstractmethod
    def is_business_day(self, d: date) -> bool:
        ...

    @abstractmethod
    def business_days_between(self, start: date, end: date) -> int:
        """Diferença de bd_index entre duas datas."""
        ...

    @abstractmethod
    def adjust_to_next_business_day(self, d: date) -> date:
        ...

    @abstractmethod
    def first_business_day_of_month(self, year: int, month: int) -> date:
        ...
        