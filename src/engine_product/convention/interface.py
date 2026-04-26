from abc import ABC, abstractmethod
from datetime import date

class DayCountConventionRepository(ABC):

    @abstractmethod
    def day_count(self, start: date, end: date) -> int:
        """Quantidade de dias entre duas datas, segundo a convenção."""
        ...

    @abstractmethod
    def year_fraction(self, start: date, end: date) -> float:
        """Fração de ano para precificação."""
        ...