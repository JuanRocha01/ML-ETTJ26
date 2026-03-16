from __future__ import annotations

from datetime import datetime, timezone


def parse_yyyymmdd(value: str) -> datetime:
    return datetime.strptime(value, "%Y%m%d")


def parse_int(value: str) -> int:
    value = value.strip()
    if not value:
        raise ValueError("Valor inteiro vazio.")
    return int(value)

def signed_int(raw_value: str, signal: str) -> int:
    value = parse_int(raw_value)
    if signal == "-":
        return -value
    return value

def apply_signal(value: int, signal: str) -> int:
    if signal == "-":
        return -value
    return value

def adjusted_value_from_raw(raw_value: str, signal: str, scale: int = 10_000) -> float:
    return signed_int(raw_value, signal) / scale

def make_lineage_id(
    outer_zip: str,
    inner_zip: str,
    txt_name: str,
    hash_file: str,
) -> str:
    return f"{outer_zip}|{inner_zip}|{txt_name}|{hash_file}"

def parse_adjusted_value(raw_value: str, signal: str, scale: int = 10_000) -> float:
    raw_int = parse_int(raw_value)
    signed = apply_signal(raw_int, signal)
    return signed / scale


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
