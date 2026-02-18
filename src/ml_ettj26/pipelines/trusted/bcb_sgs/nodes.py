from __future__ import annotations

from typing import Any, Mapping
import pandas as pd

from ml_ettj26.domain.bcb_sgs.meta import parse_series_meta
from ml_ettj26.domain.bcb_sgs.service import SgsIngestConfig, SgsTrustedBuilder
from ml_ettj26.domain.bcb_sgs.validate import (
    validate_sgs_points,
    validate_sgs_series_meta,
)


def _make_builder(params: Mapping[str, Any]) -> SgsTrustedBuilder:
    default_source = params.get("source", "BCB_SGS")
    series_meta = parse_series_meta(params.get("series_meta", {}), default_source=default_source)

    cfg = SgsIngestConfig(
        raw_dir=params["raw_dir"],
        source=default_source,
        series_meta=series_meta,
        on_conflict=params.get("on_conflict", "keep_last"),
    )
    return SgsTrustedBuilder(cfg)


def build_bcb_sgs_series_meta_trusted(params: Mapping[str, Any]) -> pd.DataFrame:
    return _make_builder(params).build_series_meta_df()


def build_bcb_sgs_points_trusted(params: Mapping[str, Any]) -> pd.DataFrame:
    return _make_builder(params).build_points_df()


def validate_bcb_sgs_series_meta(df: pd.DataFrame) -> pd.DataFrame:
    validate_sgs_series_meta(df)
    return df


def validate_bcb_sgs_points(df: pd.DataFrame) -> pd.DataFrame:
    validate_sgs_points(df)
    return df
