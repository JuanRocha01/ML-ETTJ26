from __future__ import annotations

import zipfile
from io import BytesIO
from pathlib import Path

import pytest

from ml_ettj26.domain.b3_DerivativeMarket.SwapMarketRates.zip_reader import (
    NestedZipReader,
    sha256_zip_member,
)


def _build_nested_zip(
    tmp_path: Path,
    *,
    ex_name: str = "TS200213.ex_",
    txt_name: str = "TaxaSwap.txt",
    txt_bytes: bytes = b"linha 1\r\nlinha 2",
    ex_prefix: bytes = b"",
) -> Path:
    inner_buffer = BytesIO()
    with zipfile.ZipFile(inner_buffer, "w", compression=zipfile.ZIP_DEFLATED) as inner_zip:
        inner_zip.writestr(txt_name, txt_bytes)

    outer_path = tmp_path / "TS200213_20200213.zip"
    with zipfile.ZipFile(outer_path, "w", compression=zipfile.ZIP_DEFLATED) as outer_zip:
        outer_zip.writestr(ex_name, ex_prefix + inner_buffer.getvalue())

    return outer_path


def test_read_embedded_txt_reads_first_ex_and_first_txt(tmp_path: Path):
    outer_path = _build_nested_zip(tmp_path)

    extracted = NestedZipReader(outer_path).read_embedded_txt()

    assert extracted.outer_zip_name == "TS200213_20200213.zip"
    assert extracted.ex_name == "TS200213.ex_"
    assert extracted.txt_name == "TaxaSwap.txt"
    assert extracted.text == "linha 1\r\nlinha 2"


def test_read_embedded_txt_finds_zip_signature_inside_ex_file(tmp_path: Path):
    outer_path = _build_nested_zip(tmp_path, ex_prefix=b"cabecalho antes do zip")

    extracted = NestedZipReader(outer_path).read_embedded_txt()

    assert extracted.txt_name == "TaxaSwap.txt"
    assert extracted.text == "linha 1\r\nlinha 2"


def test_read_embedded_txt_decodes_cp1252(tmp_path: Path):
    outer_path = _build_nested_zip(tmp_path, txt_bytes="ação".encode("cp1252"))

    extracted = NestedZipReader(outer_path).read_embedded_txt()

    assert extracted.text == "ação"


def test_open_inner_zip_and_list_txts(tmp_path: Path):
    outer_path = _build_nested_zip(tmp_path)
    reader = NestedZipReader(outer_path)

    with reader.open_inner_zip() as inner:
        assert inner.namelist() == ["TaxaSwap.txt"]

    assert reader.list_txts() == ["TaxaSwap.txt"]


def test_sha256_zip_member_hashes_txt_inside_inner_archive(tmp_path: Path):
    outer_path = _build_nested_zip(tmp_path, txt_bytes=b"abc123")

    with NestedZipReader(outer_path).open_inner_zip() as inner:
        file_hash = sha256_zip_member(inner, "TaxaSwap.txt")

    assert file_hash == "6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d23925b7f9cbb56f4b19"


def test_read_embedded_txt_raises_when_inner_zip_signature_is_missing(tmp_path: Path):
    outer_path = tmp_path / "broken.zip"
    with zipfile.ZipFile(outer_path, "w", compression=zipfile.ZIP_DEFLATED) as outer_zip:
        outer_zip.writestr("TS200213.ex_", b"conteudo sem zip interno")

    with pytest.raises(ValueError, match="ZIP interno nao encontrado"):
        NestedZipReader(outer_path).read_embedded_txt()
