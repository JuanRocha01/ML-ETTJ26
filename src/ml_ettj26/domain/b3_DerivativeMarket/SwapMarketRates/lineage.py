from __future__ import annotations

from ml_ettj26.utils.io.hash import sha256_hex


def make_lineage_id(
    outer_zip: str,
    inner_zip: str,
    txt_name: str,
    hash_file: str,
) -> str:
    payload = f"{outer_zip}|{inner_zip}|{txt_name}|{hash_file}"
    return sha256_hex(payload)
