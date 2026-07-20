from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class DailyCurveMatrix:
    """Validated date x BU-tenor view with vectorized point lookup."""

    values: pd.DataFrame

    @classmethod
    def from_frame(cls, frame: pd.DataFrame) -> "DailyCurveMatrix":
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            raise ValueError("curve must be a non-empty pandas DataFrame")

        normalized = frame.copy()
        normalized.index = pd.DatetimeIndex(
            pd.to_datetime(normalized.index, errors="raise")
        ).normalize()
        normalized.index.name = "ref_date"
        if normalized.index.has_duplicates:
            raise ValueError("curve contains duplicate reference dates")

        try:
            tenors = pd.Index(
                [int(column) for column in normalized.columns],
                dtype="int64",
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "curve columns must be business-day integer labels"
            ) from exc
        if tenors.has_duplicates or (tenors <= 0).any():
            raise ValueError(
                "curve business-day columns must be unique and positive"
            )

        order = np.argsort(tenors.to_numpy())
        normalized = normalized.iloc[:, order]
        normalized.columns = tenors[order]
        normalized = normalized.sort_index().astype("float64")
        return cls(normalized)

    @property
    def dates(self) -> pd.DatetimeIndex:
        return self.values.index

    @property
    def tenors(self) -> pd.Index:
        return self.values.columns

    def lookup(
        self,
        dates: Iterable,
        tenors_bd: Iterable,
    ) -> np.ndarray:
        """Return curve rates for paired dates/tenors; missing keys become NaN."""

        date_index = pd.DatetimeIndex(pd.to_datetime(list(dates))).normalize()
        tenor_index = pd.Index(
            pd.to_numeric(pd.Series(list(tenors_bd)), errors="coerce")
        )
        date_positions = self.dates.get_indexer(date_index)
        tenor_positions = self.tenors.get_indexer(tenor_index)
        result = np.full(len(date_positions), np.nan, dtype=np.float64)
        valid = (date_positions >= 0) & (tenor_positions >= 0)
        result[valid] = self.values.to_numpy(copy=False)[
            date_positions[valid],
            tenor_positions[valid],
        ]
        return result

    def selected_tenors(self, step_bd: int) -> pd.DataFrame:
        if step_bd <= 0:
            raise ValueError("step_bd must be strictly positive")
        selected = self.tenors[self.tenors % step_bd == 0]
        if len(selected) < 3:
            raise ValueError("PCA requires at least three selected tenors")
        return self.values.loc[:, selected]
