from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
import zipfile
import hashlib


@dataclass(frozen=True)
class EmbeddedTxtPayload:
    outer_zip: str
    inner_zip: str
    txt_name: str
    text: str
    hash_file: str


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class NestedZipReader:
    def __init__(self, outer_zip_path: str):
        self.outer_zip_path = Path(outer_zip_path)

    def read_embedded_txt(self, encoding: str = "cp1252") -> EmbeddedTxtPayload:
        with zipfile.ZipFile(self.outer_zip_path) as outer:
            inner_zip_name = outer.namelist()[0]
            inner_zip_bytes = outer.read(inner_zip_name)

        start = inner_zip_bytes.find(b"PK\x03\x04")
        if start == -1:
            raise ValueError(f"ZIP interno nao encontrado em {self.outer_zip_path.name}")

        real_inner_zip_bytes = inner_zip_bytes[start:]

        with zipfile.ZipFile(BytesIO(real_inner_zip_bytes)) as inner:
            txt_name = inner.namelist()[0]
            txt_bytes = inner.read(txt_name)

        return EmbeddedTxtPayload(
            outer_zip=self.outer_zip_path.name,
            inner_zip=inner_zip_name,
            txt_name=txt_name,
            text=txt_bytes.decode(encoding),
            hash_file=sha256_bytes(txt_bytes),
        ) 

