from __future__ import annotations

import pandas as pd

from ml_ettj26.infraestructure.duckdb.connections import get_connection


def build_demab_refined() -> pd.DataFrame:
    """
    Build a refined DEMAB dataset from trusted DuckDB views.

    Returns
    -------
    pd.DataFrame
        Refined DEMAB dataframe.
    """
    con = get_connection()

    try:
        query = """
        WITH

        meta AS (
            SELECT
                isin,
                sigla AS name,
                emissao_date AS issue_date,
                vencimento_date AS maturity,
                source
            FROM vw_trusted_bcb_demab_instruments
        ),

        points AS (
            SELECT
                trade_date AS date,
                isin,
                pu_min,
                pu_med,
                pu_max,
                pu_lastro,
                valor_par,
                taxa_min,
                taxa_med,
                taxa_max,
                raw_zip_hash AS raw_hash
            FROM vw_trusted_bcb_demab_quotes
        )

        SELECT
            p.date,
            m.isin,
            m.name,
            p.pu_min,
            p.pu_med,
            p.pu_max,
            p.taxa_min,
            p.taxa_med,
            p.taxa_max,
            p.pu_lastro,
            p.valor_par,
            m.issue_date,
            m.maturity,
            m.source,
            p.raw_hash
        FROM meta AS m
        JOIN points AS p
        USING (isin)
        ORDER BY p.date
        """

        df = con.execute(query).fetchdf()

    finally:
        con.close()

    return df