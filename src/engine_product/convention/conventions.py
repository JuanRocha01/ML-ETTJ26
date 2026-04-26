from datetime import date
from .interface import DayCountConventionRepository
from engine_product.calendars.business_calendar import BusinessCalendar

class BU252(DayCountConventionRepository):

    def __init__(self, calendar: BusinessCalendar):
        self._calendar = calendar

    def day_count(self, start: date, end: date) -> int:
        return self._calendar.business_days_between(start, end)

    def year_fraction(self, start: date, end: date) -> float:
        return self.day_count(start, end) / 252
