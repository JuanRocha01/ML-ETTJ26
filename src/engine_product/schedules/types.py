from datetime import date
from typing import Callable


DateList = list[date]

DateRule = Callable[[date, date], DateList]
DateAdjuster = Callable[[date], date]
ScheduleStep = Callable[[DateList], DateList]