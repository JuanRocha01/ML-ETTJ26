from datetime import date

import pandas as pd
import pytest

from engine_product.calendars.repository import DataFrameCalendarRepository
from engine_product.calendars.business_calendar import BusinessCalendar


@pytest.fixture
def calendar_df():
    return pd.DataFrame(
        [
            {
                "calendar_id": "BR_ANBIMA",
                "date": date(2026, 1, 1),
                "year": 2026,
                "month": 1,
                "day": 1,
                "weekday": 3,
                "is_weekend": False,
                "is_holiday": True,
                "is_business_day": False,
                "act_index": 0,
                "bd_index": 0,
                "holiday_name": "Confraternização Universal",
                "source_file_hash": "hash",
            },
            {
                "calendar_id": "BR_ANBIMA",
                "date": date(2026, 1, 2),
                "year": 2026,
                "month": 1,
                "day": 2,
                "weekday": 4,
                "is_weekend": False,
                "is_holiday": False,
                "is_business_day": True,
                "act_index": 1,
                "bd_index": 1,
                "holiday_name": None,
                "source_file_hash": "hash",
            },
            {
                "calendar_id": "BR_ANBIMA",
                "date": date(2026, 1, 3),
                "year": 2026,
                "month": 1,
                "day": 3,
                "weekday": 5,
                "is_weekend": True,
                "is_holiday": False,
                "is_business_day": False,
                "act_index": 2,
                "bd_index": 1,
                "holiday_name": None,
                "source_file_hash": "hash",
            },
            {
                "calendar_id": "BR_ANBIMA",
                "date": date(2026, 1, 4),
                "year": 2026,
                "month": 1,
                "day": 4,
                "weekday": 6,
                "is_weekend": True,
                "is_holiday": False,
                "is_business_day": False,
                "act_index": 3,
                "bd_index": 1,
                "holiday_name": None,
                "source_file_hash": "hash",
            },
            {
                "calendar_id": "BR_ANBIMA",
                "date": date(2026, 1, 5),
                "year": 2026,
                "month": 1,
                "day": 5,
                "weekday": 0,
                "is_weekend": False,
                "is_holiday": False,
                "is_business_day": True,
                "act_index": 4,
                "bd_index": 2,
                "holiday_name": None,
                "source_file_hash": "hash",
            },
        ]
    )


@pytest.fixture
def calendar_repo(calendar_df):
    return DataFrameCalendarRepository(calendar_df)


@pytest.fixture
def business_calendar(calendar_repo):
    return BusinessCalendar(calendar_repo)