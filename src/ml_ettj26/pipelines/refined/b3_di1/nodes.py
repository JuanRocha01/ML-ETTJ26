from __future__ import annotations

import pandas as pd

from ml_ettj26.infraestructure.duckdb.connections import get_connection

def buil_b3_di1_refined() -> pd.DataFrame:

    con = get_connection()
    try:
        query = """
        WITH

        meta AS (
        SELECT *
        FROM vw_trusted_b3_forwards_metadata
        ),

        points AS (
        SELECT *
        FROM  vw_trusted_b3_forwards_di1_quotes
        )

        SELECT
            p.TradDt AS date,
            m.asset,
            p.TckrSymb AS ticker,
            p.AdjstdQtTax AS adjusted_price,
            p.AdjstdQt AS adjusted_pu,
            m.maturity_date,
            p.BestBidPric AS bid_price,
            p.BestAskPric AS ask_price,
            p.LastPric AS close_price,
            p.TradAvrgPric AS average_price,
            p.MinPric AS minimum_price,
            p.MaxPric AS maximum_price,
            p.TradQty AS quantity,
        FROM points AS p
        LEFT JOIN meta AS m
        USING (TckrSymb)
        ORDER BY date ASC, maturity_date ASC
        """

        df = con.execute(query).fetchdf()

    finally:
        con.close()
    
    return(df)
