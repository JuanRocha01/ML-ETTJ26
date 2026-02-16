from __future__ import annotations

from typing import Mapping, Any
import pandas as pd

from ml_ettj26.domain.sgs.service import SgsIngestConfig, SgsTrustedBuilder
from ml_ettj26.domain.sgs.validate import validate_sgs_daily


def build_bcb_sgs_trusted(params: Mapping[str, Any]) -> pd.DataFrame:
    cfg = SgsIngestConfig(
        raw_dir=params["raw_dir"],
        source=params.get("source", "BCB_SGS"),
        frequency=params.get("frequency", "D"),
    )

    return SgsTrustedBuilder(cfg).build()

def validate_bcb_sgs_trusted(df: pd.DataFrame) -> pd.DataFrame:
    validate_sgs_daily(df)
    return df

