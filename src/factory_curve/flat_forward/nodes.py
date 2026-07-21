from __future__ import annotations

from typing import Any

import pandas as pd

from .interpolation import interpolate_flat_forward_batch


def build_public_bonds_flat_forward_curves(
    curve_inputs: pd.DataFrame,
    parameters: dict[str, Any],
) -> pd.DataFrame:
    """Build daily public-bond curves on every BU/252 tenor in one batch."""

    return interpolate_flat_forward_batch(
        curve_inputs,
        start_date=parameters.get("start_date", "2020-01-01"),
        max_years=int(parameters.get("max_years", 20)),
        business_days_per_year=int(
            parameters.get("business_days_per_year", 252)
        ),
        instrument_types=parameters.get("instrument_types", ("LTN", "NTN-F")),
        batch_size=int(parameters.get("batch_size", 64)),
    )
