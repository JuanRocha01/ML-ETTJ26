CREATE OR REPLACE VIEW mart_public_bonds_curve_candidates AS
SELECT *
FROM mart_public_bonds_quotes_quality
WHERE eligible_for_curve_input = TRUE;


CREATE OR REPLACE VIEW mart_public_bonds_curve_exclusions AS
SELECT *
FROM mart_public_bonds_quotes_quality
WHERE eligible_for_curve_input = FALSE;