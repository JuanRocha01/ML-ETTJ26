from datetime import date
from dateutil.relativedelta import relativedelta
from .interface import BusinessCalendarRepository

class BusinessCalendar:

    def __init__(self, repo: BusinessCalendarRepository):
        self._repo = repo

    # Delega as operações base
    def is_business_day(self, d: date) -> bool:
        return self._repo.is_business_day(d)

    def business_days_between(self, start: date, end: date) -> int:
        return self._repo.business_days_between(start, end)

    def adjust_to_next_business_day(self, d: date) -> date:
        return self._repo.adjust_to_next_business_day(d)

    def first_business_day_of_month(self, year: int, month: int) -> date:
        return self._repo.first_business_day_of_month(year, month)
