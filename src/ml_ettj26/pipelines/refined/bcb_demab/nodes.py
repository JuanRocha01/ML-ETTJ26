from __future__ import annotations

import pandas as pd

from ml_ettj26.infraestructure.duckdb.connections import get_connection


def build_demab_refined() -> pd.DataFrame:
    con = get_connection()

    try:
        query = """
        WITH

        meta AS (
            SELECT
                isin,
                sigla,
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
        ),

        base AS (
            SELECT
                p.date,
                m.isin,
                m.sigla,
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
                p.raw_hash,

                (p.pu_lastro IS NOT NULL) AS has_pu_lastro,
                (p.valor_par IS NOT NULL) AS has_valor_par,
                (
                    p.taxa_min IS NOT NULL
                    OR p.taxa_med IS NOT NULL
                    OR p.taxa_max IS NOT NULL
                ) AS has_any_taxa_reported,

                CASE
                    WHEN m.sigla = 'LTN' THEN 'zero_coupon'
                    WHEN m.sigla = 'NTN-F' THEN 'fixed_coupon'
                    WHEN m.sigla IN ('NTN-B', 'NTN-C') THEN 'inflation_linked'
                    WHEN m.sigla IN ('NTN-A3','NTN-A6', 'NTN-D') THEN 'currency_linked'
                    WHEN m.sigla IN ('LFT', 'LFT-A', 'LFT-B') THEN 'floating_rate'
                    ELSE 'unknown'
                END AS instrument_family,

                CASE
                    WHEN m.sigla = 'LTN'
                    AND p.pu_lastro IS NOT NULL
                    AND p.valor_par IS NOT NULL
                    THEN TRUE

                    WHEN m.sigla = 'NTN-F'
                    AND p.pu_lastro IS NOT NULL
                    THEN TRUE

                    ELSE FALSE
                END AS rebuild_candidate

            FROM points AS p
            LEFT JOIN meta AS m
                USING (isin)
        )

        SELECT
            b.date,
            b.isin,
            b.sigla,
            b.pu_lastro,
            b.valor_par,
            b.issue_date,
            b.maturity,

            DATE_DIFF('day', b.date, b.maturity) AS days_to_maturity,
            cal_maturity.bd_index - cal_trade.bd_index AS bd_to_maturity,

            b.pu_min,
            b.pu_med,
            b.pu_max,
            b.taxa_min,
            b.taxa_med,
            b.taxa_max,
            
            b.source,
            b.raw_hash,

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

    return df