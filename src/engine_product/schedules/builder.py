# engine_product/schedules/builder.py

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from engine_product.schedules.types import (
    DateAdjuster,
    DateRule,
    DateList,
    ScheduleStep,
)


@dataclass
class ScheduleBuilder:
    """
    Builder funcional para schedules.
    ScheduleBuilder
    não sabe o que é NTN-F, CRA, CRI, CDS ou swap

    Ele só sabe montar uma sequência:

    1. seed        → cria datas-base
    2. filter      → filtra intervalo
    3. add         → adiciona boundaries
    4. adjust      → ajusta dia útil
    5. normalize   → ordena/remove duplicadas
    6. build       → retorna o schedule final
    """

    _steps: list[ScheduleStep] = field(default_factory=list)

    def seed(
        self,
        rule: DateRule,
        start: date,
        maturity: date,
    ) -> ScheduleBuilder:
        """
        Primeiro passo: gerar datas-base.
        """
        self._steps.append(lambda _: rule(start, maturity))
        return self

    def filter_between(
        self,
        start: date,
        maturity: date,
        include_start: bool = False,
        include_maturity: bool = True,
    ) -> ScheduleBuilder:
        """
        Remove datas fora do intervalo contratual.
        """
        def step(dates: DateList) -> DateList:
            result = []

            for d in dates:
                after_start = d >= start if include_start else d > start
                before_maturity = d <= maturity if include_maturity else d < maturity

                if after_start and before_maturity:
                    result.append(d)

            return result

        self._steps.append(step)
        return self

    def add_dates(self, *dates_to_add: date) -> ScheduleBuilder:
        """
        Adiciona datas manualmente.
        Útil para incluir start_date, maturity_date ou stubs.
        """
        self._steps.append(lambda dates: dates + list(dates_to_add))
        return self

    def adjust(self, adjuster: DateAdjuster) -> ScheduleBuilder:
        """
        Ajusta cada data por uma convenção de dia útil.
        """
        self._steps.append(lambda dates: [adjuster(d) for d in dates])
        return self

    def normalize(self) -> ScheduleBuilder:
        """
        Ordena e remove duplicadas.
        """
        self._steps.append(lambda dates: sorted(set(dates)))
        return self

    def build(self) -> list[date]:
        """
        Executa os passos na ordem em que foram adicionados.
        """
        dates: DateList = []

        for step in self._steps:
            dates = step(dates)

        return dates