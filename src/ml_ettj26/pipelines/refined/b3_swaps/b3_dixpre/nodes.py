from __future__ import annotations

import pandas as pd

from ml_ettj26.infraestructure.duckdb.connections import get_connection

def build_swap_dipre_refined() -> pd.DataFrame:
    con = get_connection()

    try:
        query  = """
        WITH

        meta AS (
            SELECT *
            FROM vw_trusted_b3_swaps_metadata 
        ),

        points AS (
            SELECT *
            FROM vw_trusted_b3_swaps_dixpre_quotes
        )

    
        SELECT
            CAST(p.TradDt AS DATE) AS date,
            p.CodProd AS product_code,
            m.nome AS name,
            m.underlying,
            p.adjusted_value / 100 AS adjusted_value,
            p.TradDt + p.days_to_maturity * INTERVAL '1 day' AS maturity,
            p.days_to_maturity,
            p.bd_to_maturity,
            p.days_to_delivery,
            m.fixed_leg,
            m.float_leg,
            p.tipo_cotacao AS quote_type,
            p.lineage_id,
        FROM points p
        LEFT JOIN meta m
            USING (CodProd)
        ORDER BY p.TradDt ASC, maturity ASC            
        """

        df = con.execute(query).fetchdf()

    finally:
        con.close()

    return df
