import zipfile
from io import BytesIO
from contextlib import contextmanager
from typing import Iterator
import hashlib

def sha256_zip_member(
    zi: zipfile.ZipFile,
    member_name: str,
    chunk_size: int = 1024 * 1024,  # 1MB
        ) -> str:
    """
    Calcula SHA256 de um arquivo dentro do zip (streaming).

    Parameters
    ----------
    zi : zipfile.ZipFile
        Zip interno já aberto.
    member_name : str
        Nome do arquivo dentro do zip.
    chunk_size : int
        Tamanho do chunk de leitura (default 1MB).

    Returns
    -------
    str
        Hash SHA256 em formato hexadecimal.
    """
    hasher = hashlib.sha256()

    with zi.open(member_name) as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)

    return hasher.hexdigest()


class NestedZipReader:
    def __init__(self, outer_zip_path: str):
        self.outer_zip_path = outer_zip_path

    @contextmanager
    def open_inner_zip(self) -> Iterator[zipfile.ZipFile]:
        # 1) abre o zip externo, lê o zip interno como bytes
        with zipfile.ZipFile(self.outer_zip_path, "r") as outer:
            inner_name = next(n for n in outer.namelist() if n.lower().endswith(".zip"))
            inner_bytes = BytesIO(outer.read(inner_name))

        # 2) abre o zip interno a partir dos bytes e mantém aberto durante o "with"
        with zipfile.ZipFile(inner_bytes, "r") as inner:
            yield inner

    def list_xmls(self) -> list[str]:
        # conveniência: abre/fecha apenas pra listar
        with self.open_inner_zip() as inner:
            return [n for n in inner.namelist() if n.lower().endswith(".xml")]
