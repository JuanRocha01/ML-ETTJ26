CREATE OR REPLACE VIEW mart_public_bonds_quotes_quality AS
SELECT
    date AS ref_date,
    sigla AS instrument_type,
    isin,
    issue_date,
    maturity AS maturity_date,
    bd_to_maturity,

    pu_med,
    taxa_med,
    pu_lastro,

    CASE
        WHEN sigla = 'LTN' AND pu_med IS NOT NULL
            THEN 'OBSERVED_PU'

        WHEN sigla = 'LTN' AND pu_med IS NULL AND taxa_med IS NOT NULL
            THEN 'OBSERVED_YIELD'

        WHEN sigla = 'NTN-F' AND pu_med IS NOT NULL
            THEN 'OBSERVED_PU'

        ELSE 'NOT_ELIGIBLE'
    END AS quote_quality,

    CASE
        WHEN sigla = 'LTN' AND pu_med IS NOT NULL
            THEN 'PU_MED'

        WHEN sigla = 'LTN' AND pu_med IS NULL AND taxa_med IS NOT NULL
            THEN 'TAXA_MED'

        WHEN sigla = 'NTN-F' AND pu_med IS NOT NULL
            THEN 'PU_MED'

        ELSE 'NONE'
    END AS quote_source,

    CASE
        WHEN sigla = 'LTN' AND pu_med IS NOT NULL
            THEN 'PRICE'

        WHEN sigla = 'LTN' AND pu_med IS NULL AND taxa_med IS NOT NULL
            THEN 'YIELD'

        WHEN sigla = 'NTN-F' AND pu_med IS NOT NULL
            THEN 'PRICE'

        ELSE 'NONE'
    END AS primary_quote_type,

    CASE
        WHEN sigla IN ('LTN', 'NTN-F') THEN TRUE
        ELSE FALSE
    END AS can_price_in_engine,

    CASE
        WHEN sigla NOT IN ('LTN', 'NTN-F')
            THEN 'PRICING_ENGINE_NOT_AVAILABLE'

        WHEN maturity <= ref_date
            THEN 'INVALID_MATURITY'

        WHEN bd_to_maturity IS NULL OR bd_to_maturity <= 0
            THEN 'INVALID_BD_TO_MATURITY'

        WHEN sigla = 'NTN-F' AND pu_med IS NULL
            THEN 'NTNF_REQUIRES_OBSERVED_PU'

        WHEN sigla = 'LTN' AND pu_med IS NULL AND taxa_med IS NULL
            THEN 'LTN_MISSING_PU_AND_YIELD'

        ELSE NULL
    END AS exclusion_reason,

    CASE
        WHEN
            sigla = 'LTN'
            AND (pu_med IS NOT NULL OR taxa_med IS NOT NULL)
            AND maturity > ref_date
            AND bd_to_maturity > 0
        THEN TRUE

        WHEN
            sigla = 'NTN-F'
            AND pu_med IS NOT NULL
            AND maturity > ref_date
            AND bd_to_maturity > 0
        THEN TRUE

        ELSE FALSE
    END AS eligible_for_curve_input

FROM vw_refined_bcb_demab_government_bonds_secondary_market;