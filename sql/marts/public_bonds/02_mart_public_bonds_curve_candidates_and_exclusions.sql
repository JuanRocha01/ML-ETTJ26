CREATE OR REPLACE VIEW mart_public_bonds_curve_candidates AS
WITH eligible_candidates AS (
    SELECT *
    FROM mart_public_bonds_quotes_quality
    WHERE eligible_for_curve_input = TRUE
),
daily_stats AS (
    SELECT
        *,
        COUNT(*) OVER (
            PARTITION BY ref_date
        ) AS numero_observacoes_dia,
        (
            MAX(bd_to_maturity) OVER (PARTITION BY ref_date)
            - MIN(bd_to_maturity) OVER (PARTITION BY ref_date)
        ) / 252.0 AS tenor_spread_years
    FROM eligible_candidates
)
SELECT
    * EXCLUDE (tenor_spread_years),
    CASE
        WHEN numero_observacoes_dia < 8 THEN 'LOW'
        WHEN numero_observacoes_dia BETWEEN 8 AND 12 THEN 'MEDIUM'
        ELSE 'HIGH'
    END AS flag_volume,
    CASE
        WHEN tenor_spread_years < 2.0 THEN 'POOR'
        WHEN tenor_spread_years <= 5.0 THEN 'MEDIUM'
        ELSE 'GOOD'
    END AS flag_cobertura_tenors
FROM daily_stats;


CREATE OR REPLACE VIEW mart_public_bonds_curve_exclusions AS
SELECT *
FROM mart_public_bonds_quotes_quality
WHERE eligible_for_curve_input = FALSE;
