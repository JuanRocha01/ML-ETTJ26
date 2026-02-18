from __future__ import annotations

from pathlib import Path

import json
from pathlib import Path
from typing import Any, List, Dict


def parse_series_id_from_filename(path: Path) -> int:
    # exemplo: 432_01-01-2000_31-12-2008.json
    stem = path.stem
    series_str = stem.split("_", 1)[0]
    return int(series_str)

# Parser JSON: Converte conteúdo raw -> dicts
def read_sgs_json(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


