from __future__ import annotations

import pandas as pd

from ml_ettj26.infraestructure.duckdb.connections import get_connection


def build_sgs_series_refined(series_id: int) -> pd.DataFrame:
    """
    Build a refined SGS series from trusted DuckDB views.

    Parameters
    ----------
    series_id : int
        SGS series identifier. Example:
        432 = SELIC
        433 = IPCA

    Returns
    -------
    pd.DataFrame
        Refined SGS series dataframe.
    """
    con = get_connection()

    try:
        query = f"""
        WITH

        meta AS (
            SELECT
                series_id,
                series_name,
                unit,
                frequency,
                source
            FROM vw_trusted_bcb_sgs_metadata
            WHERE series_id = {series_id}
        ),

        points AS (
            SELECT
                series_id,
                ref_date,
                value,
                raw_hash
            FROM vw_trusted_bcb_sgs_points
            WHERE series_id = {series_id}
        )

        SELECT
            p.ref_date AS date,
            m.series_id,
            m.series_name,
            p.value,
            m.unit,
            m.frequency,
            m.source,
            p.raw_hash
        FROM meta AS m
        JOIN points AS p
        USING (series_id)
        ORDER BY p.ref_date
        """

        df = con.execute(query).fetchdf()

    finally:
        con.close()

    return df