CREATE OR REPLACE VIEW vw_refined_bcb_demab_government_bonds_secondary_market AS
SELECT *
FROM read_parquet('C:\Users\Dell\OneDrive\Documentos\GitHub\ML-ETTJ26\data\03_refined\bcb\demab\demab_government_bonds_secondary_market_refined.parquet');