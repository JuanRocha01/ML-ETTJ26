from __future__ import annotations


def make_lineage_id(
    outer_zip: str,
    inner_zip: str,
    txt_name: str,
    hash_file: str,
) -> str:
    return f"{outer_zip}|{inner_zip}|{txt_name}|{hash_file}"
