from __future__ import annotations

from pathlib import Path

import pandas as pd
from kedro.io import AbstractDataset


class SafeParquetDataset(AbstractDataset[pd.DataFrame, None]):
    def __init__(
        self,
        filepath: str,
        fallback_filepath: str | None = None,
        engine: str = "pyarrow",
    ):
        self._filepath = Path(filepath)
        self._fallback_filepath = Path(fallback_filepath) if fallback_filepath else None
        self._engine = engine

    def _load(self) -> pd.DataFrame:
        for candidate in self._candidate_paths():
            if not candidate.exists():
                continue
            if candidate.stat().st_size == 0:
                continue
            return pd.read_parquet(candidate, engine=self._engine)
        return pd.DataFrame()

    def _save(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("Dataset somente leitura.")

    def _describe(self) -> dict:
        return {
            "filepath": str(self._filepath),
            "fallback_filepath": str(self._fallback_filepath) if self._fallback_filepath else None,
            "engine": self._engine,
        }

    def _candidate_paths(self) -> list[Path]:
        candidates = [self._filepath]
        if self._fallback_filepath is not None:
            candidates.append(self._fallback_filepath)
        return candidates
