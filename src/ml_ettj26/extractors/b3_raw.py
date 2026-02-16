from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional
import zipfile

from ml_ettj26.extractors.b3_downloader import B3BoletimDownloader

@dataclass(frozen=True)
class B3RawPaths:
    raw_dir: Path

class B3RawExtractor:
    def __init__(self, downloader: B3BoletimDownloader, paths: B3RawPaths):
        self.downloader = downloader
        self.paths = paths
        self.paths.raw_dir.mkdir(parents=True, exist_ok=True)

    def fetch_and_store_zip(self, file_key: str, ref_date: date, out_name: Optional[str] = None) -> Path:
        content = self.downloader.download_zip(file_key=file_key, ref_date=ref_date)

        if out_name is None:
            out_name = f"b3_{file_key}_{ref_date.strftime('%Y%m%d')}.zip"

        out_path = self.paths.raw_dir / out_name
        out_path.write_bytes(content)
        return out_path

    def extract_first_file(self, zip_path: Path, out_dir: Optional[Path] = None) -> Path:
        out_dir = out_dir or zip_path.parent
        out_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(zip_path, "r") as zf:
            members = [m for m in zf.namelist() if not m.endswith("/")]
            if not members:
                raise ValueError(f"ZIP sem arquivos: {zip_path}")
            member = members[0]
            out_path = out_dir / Path(member).name
            out_path.write_bytes(zf.read(member))
        return out_path
