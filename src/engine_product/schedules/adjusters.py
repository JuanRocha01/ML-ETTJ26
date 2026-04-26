from datetime import date

from engine_product.schedules.types import DateAdjuster
from engine_product.calendars.business_calendar import BusinessCalendar


def unadjusted() -> DateAdjuster:
    def adjust(d: date) -> date:
        return d

    return adjust


def following(calendar: BusinessCalendar) -> DateAdjuster:
    def adjust(d: date) -> date:
        return calendar.adjust_next_business_day(d)

    return adjust


def preceding(calendar: BusinessCalendar) -> DateAdjuster:
    def adjust(d: date) -> date:
        return calendar.adjust_previous_business_day(d)

    return adjust


def nearest_business_day(calendar: BusinessCalendar) -> DateAdjuster:
    def adjust(d: date) -> date:
        if calendar.is_business_day(d):
            return d

        previous_bd = calendar.adjust_previous_business_day(d)
        next_bd = calendar.adjust_next_business_day(d)

        distance_to_previous = (d - previous_bd).days
        distance_to_next = (next_bd - d).days

        if distance_to_previous <= distance_to_next:
            return previous_bd

        return next_bd

    return adjust