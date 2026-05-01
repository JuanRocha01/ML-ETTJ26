from dataclasses import dataclass
from datetime import date

from engine_product.convention import DayCountConventionRepository
from engine_product.calendars.business_calendar import BusinessCalendar

from engine_product.cashflows import CashflowEngineBuilder, CashflowEvent
from engine_product.cashflows.components.interest import InterestComponent
from engine_product.cashflows.components.principal import PrincipalComponent
from engine_product.cashflows.indexers.fixed import CompoundFixedRateIndexer
from engine_product.cashflows.models import Cashflow

from engine_product.schedules import (
    ScheduleBuilder,
    custom_dates,
    first_day_of_months,
    following,
)


DEFAULT_PUBLIC_BOND_NOTIONAL = 1000.0
DEFAULT_NTNF_COUPON_RATE = 0.10
NTNF_COUPON_MONTHS = [1, 7]

@dataclass(frozen=True)
class LTNContract:
    start_date: date
    maturity_date: date
    calendar: BusinessCalendar
    notional: float = DEFAULT_PUBLIC_BOND_NOTIONAL

    def build_schedule(self) -> list[date]:
        return (
            ScheduleBuilder()
            .seed(
                rule=custom_dates([self.maturity_date]),
                start=self.start_date,
                maturity=self.maturity_date,
            )
            .adjust(following(self.calendar))
            .normalize()
            .build()
        )

    def build_events(self) -> list[CashflowEvent]:
        schedule = self.build_schedule()

        return [
            CashflowEvent(
                event_date=schedule[-1],
                interest=False,
                principal=True,
            )
        ]

    def build_cashflows(self, as_of_date: date) -> list[Cashflow]:
        return (
            CashflowEngineBuilder(
                issue_date=self.start_date,
                notional=self.notional,
                events=self.build_events(),
            )
            .add_component(PrincipalComponent())
            .build_cashflows(as_of_date=as_of_date)
        )


@dataclass(frozen=True)
class NTNFContract:
    start_date: date
    maturity_date: date
    calendar: BusinessCalendar
    day_count: DayCountConventionRepository
    notional: float = DEFAULT_PUBLIC_BOND_NOTIONAL
    coupon_rate: float = DEFAULT_NTNF_COUPON_RATE

    def build_schedule(self) -> list[date]:
        return (
            ScheduleBuilder()
            .seed(
                rule=first_day_of_months(months=NTNF_COUPON_MONTHS),
                start=self.start_date,
                maturity=self.maturity_date,
            )
            .add_dates(self.start_date, self.maturity_date)
            .adjust(following(self.calendar))
            .normalize()
            .build()
        )

    def build_events(self) -> list[CashflowEvent]:
        schedule = self.build_schedule()

        return [
            CashflowEvent(
                event_date=payment_date,
                interest=True,
                principal=payment_date == schedule[-1],
            )
            for payment_date in schedule[1:]
        ]

    def build_cashflows(self, as_of_date: date) -> list[Cashflow]:
        indexer = CompoundFixedRateIndexer(
            annual_rate=self.coupon_rate,
            day_count=self.day_count,
        )

        return (
            CashflowEngineBuilder(
                issue_date=self.start_date,
                notional=self.notional,
                events=self.build_events(),
            )
            .add_component(InterestComponent(indexer=indexer))
            .add_component(PrincipalComponent())
            .build_cashflows(as_of_date=as_of_date)
        )