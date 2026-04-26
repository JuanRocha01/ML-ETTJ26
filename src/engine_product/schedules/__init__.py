"""
Public API for schedule generation.

This module exposes schedule rules, adjusters and the ScheduleBuilder
used to construct event schedules for financial instruments.
"""

from .builder import ScheduleBuilder

from .rules import (
    custom_dates,
    every_n_months_backward,
    first_day_of_months,
    nth_weekday_of_month,
)

from .adjusters import (
    unadjusted,
    following,
    preceding,
    nearest_business_day,
)

__all__ = [
    "ScheduleBuilder",

    "custom_dates",
    "every_n_months_backward",
    "first_day_of_months",
    "nth_weekday_of_month",

    "unadjusted",
    "following",
    "preceding",
    "nearest_business_day",
]