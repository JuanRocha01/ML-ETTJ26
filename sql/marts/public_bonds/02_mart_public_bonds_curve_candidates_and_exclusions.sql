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
        -- Faixas empíricas definidas pelos quartis de bd_to_maturity:
        -- curto <= Q25 (219), médio entre Q25 e Q75, longo > Q75 (920).
        SUM(CASE WHEN bd_to_maturity <= 219 THEN 1 ELSE 0 END)
            OVER (PARTITION BY ref_date) AS numero_observacoes_curto,
        SUM(CASE WHEN bd_to_maturity > 219 AND bd_to_maturity <= 920 THEN 1 ELSE 0 END)
            OVER (PARTITION BY ref_date) AS numero_observacoes_medio,
        SUM(CASE WHEN bd_to_maturity > 920 THEN 1 ELSE 0 END)
            OVER (PARTITION BY ref_date) AS numero_observacoes_longo,
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
    END AS flag_cobertura_tenors,
    CASE
        -- GOOD exige presença absoluta e distribuição mínima nas três regiões.
        -- Isso evita classificar como boa uma curva numerosa, mas concentrada.
        WHEN numero_observacoes_curto >= 2
            AND numero_observacoes_medio >= 3
            AND numero_observacoes_longo >= 2
            AND numero_observacoes_curto * 1.0 / numero_observacoes_dia >= 0.15
            AND numero_observacoes_medio * 1.0 / numero_observacoes_dia >= 0.30
            AND numero_observacoes_longo * 1.0 / numero_observacoes_dia >= 0.15
            THEN 'GOOD'
        WHEN numero_observacoes_curto >= 1
            AND numero_observacoes_medio >= 1
            AND numero_observacoes_longo >= 1
            THEN 'MEDIUM'
        ELSE 'POOR'
    END AS flag_ocupacao_tenors
FROM daily_stats;


CREATE OR REPLACE VIEW mart_public_bonds_curve_exclusions AS
SELECT *
FROM mart_public_bonds_quotes_quality
WHERE eligible_for_curve_input = FALSE;
