from __future__ import annotations

import pandas as pd

from ml_ettj26.service.refined.reference.calendar import build_refined_dim_calendar_br_market
from ml_ettj26.infraestructure.duckdb.readers import read_view

def load_trusted_calendar_from_duckdb() -> pd.DataFrame:
    return read_view("vw_trusted_calendar_br")

def build_br_calendar_refined (trusted_calendar_df : pd.DataFrame) -> pd.DataFrame:
    return(build_refined_dim_calendar_br_market(trusted_calendar_df))
