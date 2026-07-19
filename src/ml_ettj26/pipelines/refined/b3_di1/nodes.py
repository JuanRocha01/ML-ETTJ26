from __future__ import annotations

import pandas as pd

from ml_ettj26.infraestructure.duckdb.connections import get_connection


BUILD_B3_DI1_REFINED_QUERY = """
    WITH

    meta AS (
        SELECT
            TckrSymb,
            ANY_VALUE(asset) AS asset,
            MAX(maturity_date) AS maturity_date
        FROM vw_trusted_b3_forwards_metadata
        GROUP BY TckrSymb
    ),

    points AS (
        SELECT * EXCLUDE (_row_number)
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        CAST(timezone('UTC', TradDt) AS DATE),
                        TckrSymb
                    ORDER BY
                        snapshot_ts_utc DESC NULLS LAST,
                        ingestion_ts_utc DESC NULLS LAST
                ) AS _row_number
            FROM vw_trusted_b3_forwards_di1_quotes
        )
        WHERE _row_number = 1
    ),

    base AS (
        SELECT
            CAST(timezone('UTC', p.TradDt) AS DATE) AS date,
            m.asset,
            p.TckrSymb AS ticker,
            p.AdjstdQtTax AS adjusted_price,
            p.AdjstdQt AS adjusted_pu,
            CAST(timezone('UTC', m.maturity_date) AS DATE) AS maturity,
            p.BestBidPric AS bid_price,
            p.BestAskPric AS ask_price,
            p.LastPric AS close_price,
            p.TradAvrgPric AS average_price,
            p.MinPric AS minimum_price,
            p.MaxPric AS maximum_price,
            p.TradQty AS quantity
        FROM points AS p
        INNER JOIN meta AS m
            USING (TckrSymb)
    ),

    calendarized AS (
        SELECT
            b.*,
            DATE_DIFF('day', b.date, b.maturity) AS days_to_maturity,
            cal_maturity.bd_index - cal_trade.bd_index AS bd_to_maturity
        FROM base AS b
        INNER JOIN vw_refined_calendar_br AS cal_trade
            ON b.date = cal_trade.date
            AND cal_trade.is_business_day = TRUE
        INNER JOIN vw_refined_calendar_br AS cal_maturity
            ON b.maturity = cal_maturity.date
            AND cal_maturity.is_business_day = TRUE
    ),

    valid_contracts AS (
        SELECT *
        FROM calendarized
        WHERE maturity > date
            AND days_to_maturity > 0
            AND bd_to_maturity > 0
            AND adjusted_price IS NOT NULL
            AND ISFINITE(adjusted_price)
            AND adjusted_price > -100.0
            AND adjusted_pu IS NOT NULL
            AND ISFINITE(adjusted_pu)
            AND adjusted_pu > 0.0
    )

    SELECT
        date,
        asset,
        ticker,
        adjusted_price,
        adjusted_pu,
        maturity,
        days_to_maturity,
        bd_to_maturity,

        CASE
            WHEN bid_price IS NULL
                OR ask_price IS NULL
                OR bid_price <= ask_price
                THEN bid_price
            ELSE NULL
        END AS bid_price,

        CASE
            WHEN bid_price IS NULL
                OR ask_price IS NULL
                OR bid_price <= ask_price
                THEN ask_price
            ELSE NULL
        END AS ask_price,

        CASE
            WHEN close_price IS NULL THEN NULL
            WHEN minimum_price IS NULL OR maximum_price IS NULL THEN close_price
            WHEN minimum_price <= close_price AND close_price <= maximum_price
                THEN close_price
            ELSE NULL
        END AS close_price,

        CASE
            WHEN average_price IS NULL THEN NULL
            WHEN minimum_price IS NULL OR maximum_price IS NULL THEN average_price
            WHEN minimum_price <= average_price AND average_price <= maximum_price
                THEN average_price
            ELSE NULL
        END AS average_price,

        CASE
            WHEN minimum_price IS NULL
                OR maximum_price IS NULL
                OR minimum_price <= maximum_price
                THEN minimum_price
            ELSE NULL
        END AS minimum_price,

        CASE
            WHEN minimum_price IS NULL
                OR maximum_price IS NULL
                OR minimum_price <= maximum_price
                THEN maximum_price
            ELSE NULL
        END AS maximum_price,

        CASE
            WHEN quantity IS NULL OR quantity >= 0 THEN quantity
            ELSE NULL
        END AS quantity

    FROM valid_contracts
    ORDER BY date ASC, maturity ASC
"""


def buil_b3_di1_refined() -> pd.DataFrame:

    con = get_connection(read_only=True)
    try:
        df = con.execute(BUILD_B3_DI1_REFINED_QUERY).fetchdf()

    finally:
        con.close()

    return df
