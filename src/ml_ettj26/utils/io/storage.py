from __future__ import annotations

from typing import Protocol
import os


class ByteStorage(Protocol):
    def save(self, relative_path: str, content: bytes) -> str: ...


class LocalFileStorage:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir

    def save(self, relative_path: str, content: bytes) -> str:
        full_path = os.path.join(self.base_dir, relative_path)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        with open(full_path, "wb") as f:
            f.write(content)
        return full_path
