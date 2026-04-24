CREATE OR REPLACE VIEW vw_refined_b3_forwards_di1 AS
SELECT *
FROM read_parquet('C:\Users\Dell\OneDrive\Documentos\GitHub\ML-ETTJ26\data\03_refined\b3\forwards\b3_forwards_di1_refined.parquet');