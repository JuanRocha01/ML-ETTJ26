CREATE OR REPLACE VIEW vw_refined_b3_swaps_di_pre AS
SELECT *
FROM read_parquet('C:\Users\Dell\OneDrive\Documentos\GitHub\ML-ETTJ26\data\03_refined\b3\swaps\b3_swaps_dipre_refined.parquet');