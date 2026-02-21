from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional, Set

def parse_anbima_holidays_csv(path: str | Path, encoding: str = "utf-8") -> Set[date]:
    """
    Lê o CSV da ANBIMA (ex.: 'Data;Dia da Semana;Feriado;...')
    e retorna um set de datas de feriados.
    """
    holidays: Set[date] = set()
    path = Path(path)

    with path.open("r", encoding=encoding, errors="replace") as f:
        header = f.readline()  # descarta cabeçalho
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(";")
            dt_str = parts[0].strip()  # '01/01/2001'
            try:
                dt = datetime.strptime(dt_str, "%d/%m/%Y").date()
            except ValueError:
                # se tiver linhas estranhas, ignora
                continue
            holidays.add(dt)
    return holidays

@dataclass(frozen=True)
class BusinessDayCalendar:
    holidays: Set[date]

    def is_business_day(self, d: date) -> bool:
        # weekday: 0=segunda ... 5=sábado 6=domingo
        if d.weekday() >= 5:
            return False
        if d in self.holidays:
            return False
        return True

    def business_days_between(self, start: date, end: date, *, include_start: bool = False, include_end: bool = True) -> int:
        """
        Conta dias úteis entre start e end.
        Padrão comum em finanças BR: (start, end] => include_start=False, include_end=True
        """
        if end < start:
            start, end = end, start

        # ajusta limites conforme inclusão
        cur = start if include_start else (start + timedelta(days=1))
        last = end if include_end else (end - timedelta(days=1))

        if last < cur:
            return 0

        n = 0
        d = cur
        while d <= last:
            if self.is_business_day(d):
                n += 1
            d += timedelta(days=1)
        return n
