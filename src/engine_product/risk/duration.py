from datetime import date

from engine_product.cashflows.models import Cashflow
from engine_product.convention import DayCountConventionRepository


def macaulay_duration(
    cashflows: list[Cashflow],
    ytm: float,
    settlement_date: date,
    day_count: DayCountConventionRepository,
) -> float:
    """
    Computes Macaulay duration under annual compound yield.

    D_mac = sum(t_i * PV_i) / sum(PV_i)
    """

    if ytm <= -1.0:
        raise ValueError("ytm must be greater than -1")

    weighted_time_sum = 0.0
    price = 0.0

    for cf in cashflows:
        if cf.payment_date <= settlement_date:
            continue

        t = day_count.year_fraction(settlement_date, cf.payment_date)

        if t <= 0:
            continue

        pv = cf.amount / ((1.0 + ytm) ** t)

        weighted_time_sum += t * pv
        price += pv

    if price <= 0:
        raise ValueError("price must be positive to compute duration")

    return weighted_time_sum / price


def modified_duration(
    cashflows: list[Cashflow],
    ytm: float,
    settlement_date: date,
    day_count: DayCountConventionRepository,
) -> float:
    """
    Computes modified duration under annual compound yield.

    D_mod = D_mac / (1 + y)
    """

    mac_duration = macaulay_duration(
        cashflows=cashflows,
        ytm=ytm,
        settlement_date=settlement_date,
        day_count=day_count,
    )

    return mac_duration / (1.0 + ytm)

def modified_duration_from_derivative(
    cashflows: list[Cashflow],
    ytm: float,
    settlement_date: date,
    day_count: DayCountConventionRepository,
) -> float:
    """
    Computes modified duration as:

        D_mod = - (1 / P) * dP/dy

    For compound discounting:

        P = sum(CF_i / (1 + y) ** t_i)
        dP/dy = sum(-t_i * CF_i / (1 + y) ** (t_i + 1))
    """

    if ytm <= -1.0:
        raise ValueError("ytm must be greater than -1")

    price = 0.0
    derivative = 0.0

    for cf in cashflows:
        if cf.payment_date <= settlement_date:
            continue

        t = day_count.year_fraction(settlement_date, cf.payment_date)

        if t <= 0:
            continue

        price += cf.amount / ((1.0 + ytm) ** t)
        derivative += -t * cf.amount / ((1.0 + ytm) ** (t + 1.0))

    if price <= 0:
        raise ValueError("price must be positive to compute duration")

    return -derivative / price