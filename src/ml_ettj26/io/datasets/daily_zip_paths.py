from pathlib import Path
from kedro.io import AbstractDataset

class DailyZipPathsDataset(AbstractDataset[list[str], None]):
    def __init__(self, folder: str, glob_pattern: str = "*.zip"):
        self._folder = Path(folder)
        self._glob = glob_pattern

    def _load(self) -> list[str]:
        paths = sorted(str(p) for p in self._folder.glob(self._glob))
        return paths

    def _save(self, data) -> None:
        raise NotImplementedError("Dataset somente leitura.")

    def _describe(self) -> dict:
        return {"folder": str(self._folder), "glob_pattern": self._glob}
