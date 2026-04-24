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
                FROM vw_trusted_b3_forwards_di1_quotes
            ),

            base AS (
                SELECT
                    CAST(p.TradDt AS DATE) AS date,
                    m.asset,
                    p.TckrSymb AS ticker,
                    p.AdjstdQtTax AS adjusted_price,
                    p.AdjstdQt AS adjusted_pu,
                    CAST(m.maturity_date AS DATE) AS maturity,
                    p.BestBidPric AS bid_price,
                    p.BestAskPric AS ask_price,
                    p.LastPric AS close_price,
                    p.TradAvrgPric AS average_price,
                    p.MinPric AS minimum_price,
                    p.MaxPric AS maximum_price,
                    p.TradQty AS quantity
                FROM points AS p
                LEFT JOIN meta AS m
                    USING (TckrSymb)
            )

            SELECT
                b.date,
                b.asset,
                b.ticker,
                b.adjusted_price,
                b.adjusted_pu,
                b.maturity,

                DATE_DIFF('day', b.date, b.maturity) AS days_to_maturity,
                cal_maturity.bd_index - cal_trade.bd_index AS bd_to_maturity,

                b.bid_price,
                b.ask_price,
                b.close_price,
                b.average_price,
                b.minimum_price,
                b.maximum_price,
                b.quantity

            FROM base AS b

            LEFT JOIN vw_refined_calendar_br AS cal_trade
                ON b.date = CAST(cal_trade.date AS DATE)

            LEFT JOIN vw_refined_calendar_br AS cal_maturity
                ON b.maturity = CAST(cal_maturity.date AS DATE)

            ORDER BY b.date ASC, b.maturity ASC
            """

        df = con.execute(query).fetchdf()

    finally:
        con.close()
    
    return(df)
