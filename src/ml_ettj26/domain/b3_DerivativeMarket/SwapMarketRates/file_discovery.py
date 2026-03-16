from __future__ import annotations

import re
from datetime import datetime, date
from pathlib import Path


_RE_FILE_DATE = re.compile(r"_(\d{8})\.zip$", re.IGNORECASE)


def parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def extract_file_date(filepath: Path) -> date | None:
    match = _RE_FILE_DATE.search(filepath.name)
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y%m%d").date()


def list_zip_files_in_date_range(
    raw_dir: str | Path,
    start_date: str,
    end_date: str,
) -> list[Path]:
    base_path = Path(raw_dir)
    dt_start = parse_iso_date(start_date)
    dt_end = parse_iso_date(end_date)

    if dt_end < dt_start:
        raise ValueError(
            f"end_date ({end_date}) não pode ser menor que start_date ({start_date})."
        )

    selected_files: list[Path] = []

    for path in sorted(base_path.glob("*.zip")):
        file_date = extract_file_date(path)
        if file_date is None:
            continue
        if dt_start <= file_date <= dt_end:
            selected_files.append(path)

    return selected_files