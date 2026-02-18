from __future__ import annotations

from pathlib import Path
import zipfile


def get_single_csv_name(zip_path: Path) -> str:
    with zipfile.ZipFile(zip_path, "r") as z:
        csvs = [i.filename for i in z.infolist() if not i.is_dir() and i.filename.lower().endswith(".csv")]
        if len(csvs) != 1:
            raise ValueError(f"Expected exactly 1 CSV inside {zip_path.name}, found {len(csvs)}: {csvs[:5]}")
        return csvs[0]


def open_csv_stream(zip_path: Path, inner_csv: str):
    z = zipfile.ZipFile(zip_path, "r")
    # o chamador fecha: stream.close(); z.close()
    return z, z.open(inner_csv)
