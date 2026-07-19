from __future__ import annotations

import pandas as pd

from ml_ettj26.infraestructure.duckdb.connections import get_connection


BUILD_SWAP_DIPRE_REFINED_QUERY = """
    WITH

    meta AS (
        SELECT
            CodProd,
            ANY_VALUE(nome) AS nome,
            ANY_VALUE(underlying) AS underlying,
            ANY_VALUE(fixed_leg) AS fixed_leg,
            ANY_VALUE(float_leg) AS float_leg
        FROM vw_trusted_b3_swaps_metadata
        GROUP BY CodProd
    ),

    base AS (
        SELECT
            CAST(p.TradDt AS DATE) AS date,
            p.CodProd AS product_code,
            m.nome AS name,
            m.underlying,
            p.adjusted_value / 100 AS adjusted_value,
            CAST(
                p.TradDt + p.days_to_maturity * INTERVAL '1 day'
                AS DATE
            ) AS maturity,
            p.bd_to_maturity AS source_bd_to_maturity,
            p.days_to_delivery,
            m.fixed_leg,
            m.float_leg,
            p.tipo_cotacao AS quote_type,
            p.lineage_id
        FROM vw_trusted_b3_swaps_dixpre_quotes AS p
        LEFT JOIN meta AS m
            ON p.CodProd = m.CodProd
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
    )

    SELECT
        date,
        product_code,
        name,
        underlying,
        adjusted_value,
        maturity,
        days_to_maturity,
        bd_to_maturity,
        source_bd_to_maturity,
        days_to_delivery,
        fixed_leg,
        float_leg,
        quote_type,
        lineage_id
    FROM calendarized
    WHERE maturity > date
        AND days_to_maturity > 0
        AND bd_to_maturity > 0
        AND adjusted_value IS NOT NULL
        AND ISFINITE(adjusted_value)
        AND adjusted_value > -100.0
    ORDER BY date ASC, maturity ASC
"""


def build_swap_dipre_refined() -> pd.DataFrame:
    con = get_connection(read_only=True)

    try:
        return con.execute(BUILD_SWAP_DIPRE_REFINED_QUERY).fetchdf()

    finally:
        con.close()
