from __future__ import annotations

import pandas as pd

from ml_ettj26.service.refined.reference.calendar import build_refined_dim_calendar_br_market

def build_br_calendar_refined (trusted_calendar_df : pd.DataFrame) -> pd.DataFrame:
    return(build_refined_dim_calendar_br_market(trusted_calendar_df))

