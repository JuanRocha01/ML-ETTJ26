from datetime import date, timedelta
from collections.abc import Iterable

from dateutil.relativedelta import relativedelta

from engine_product.schedules.types import DateRule


def custom_dates(dates: Iterable[date]) -> DateRule:
    """
    Usa datas já especificadas contratualmente.
    Exemplo: CRI, CRA, debênture com agenda da escritura.
    """
    fixed_dates = list(dates)

    def rule(start: date, maturity: date) -> list[date]:
        return [
            d for d in fixed_dates
            if start < d <= maturity
        ]

    return rule


def every_n_months_backward(months: int) -> DateRule:
    """
    Gera datas voltando da maturidade para o início.

    Bom para instrumentos em que a maturidade é a âncora contratual.
    """
    if months <= 0:
        raise ValueError("months must be positive")

    def rule(start: date, maturity: date) -> list[date]:
        dates: list[date] = []
        current = maturity

        while current > start:
            dates.append(current)
            current = current - relativedelta(months=months)

        return dates

    return rule


def first_day_of_months(months: Iterable[int]) -> DateRule:
    """
    Gera o primeiro dia dos meses informados.
    Exemplo: janeiro e julho.
    """
    selected_months = list(months)

    def rule(start: date, maturity: date) -> list[date]:
        dates: list[date] = []

        for year in range(start.year, maturity.year + 1):
            for month in selected_months:
                candidate = date(year, month, 1)

                if start < candidate <= maturity:
                    dates.append(candidate)

        return dates

    return rule


def nth_weekday_of_month(
    months: Iterable[int],
    weekday: int,
    n: int,
) -> DateRule:
    """
    Gera o n-ésimo weekday de cada mês.

    weekday:
        segunda = 0
        terça   = 1
        quarta  = 2
        quinta  = 3
        sexta   = 4
        sábado  = 5
        domingo = 6
    """
    selected_months = list(months)

    if not 0 <= weekday <= 6:
        raise ValueError("weekday must be between 0 and 6")

    if n <= 0:
        raise ValueError("n must be positive")

    def rule(start: date, maturity: date) -> list[date]:
        dates: list[date] = []

        for year in range(start.year, maturity.year + 1):
            for month in selected_months:
                candidate = _nth_weekday(year, month, weekday, n)

                if start < candidate <= maturity:
                    dates.append(candidate)

        return dates

    return rule


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first_day = date(year, month, 1)
    offset = (weekday - first_day.weekday()) % 7
    first_target_weekday = first_day + timedelta(days=offset)

    return first_target_weekday + timedelta(weeks=n - 1)