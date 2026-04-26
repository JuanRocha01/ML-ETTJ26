import pandas as pd
from datetime import date
from .interface import BusinessCalendarRepository

class DataFrameCalendarRepository(BusinessCalendarRepository):

    def __init__(self, df: pd.DataFrame):
        # Indexa por data para lookup O(1)
        self._data = df.set_index("date")

    def get(self, d: date) -> dict:
        return self._data.loc[d].to_dict()
    
    def actual_days_between(self, start: date, end: date) -> int:
        act_start = self._data.loc[start, "act_index"]
        act_end = self._data.loc[end, "act_index"]
        return int(act_end - act_start)

    def is_business_day(self, d: date) -> bool:
        return bool(self._data.loc[d, "is_business_day"])

    def business_days_between(self, start: date, end: date) -> int:
        bd_start = self._data.loc[start, "bd_index"]
        bd_end = self._data.loc[end, "bd_index"]
        return int(bd_end - bd_start)

    def adjust_to_next_business_day(self, d: date) -> date:
        mask = (
            (self._data.index >= d) &
            (self._data["is_business_day"] == True)
        )
        return self._data[mask].index[0]

    def first_business_day_of_month(self, year: int, month: int) -> date:
        mask = (
            (self._data["year"] == year) &
            (self._data["month"] == month) &
            (self._data["is_business_day"] == True)
        )
        return self._data[mask].index[0]