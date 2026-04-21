from __future__ import annotations

import pandas as pd

from ml_ettj26.infraestructure.duckdb.connections import get_connection


def read_view(view_name: str) -> pd.DataFrame:
    con = get_connection()
    try:
        return con.sql(f"SELECT * FROM {view_name}").df()
    finally:
        con.close()