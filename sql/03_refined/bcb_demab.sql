CREATE OR REPLACE VIEW vw_refined_bcb_demab_government_bonds_secondary_market AS
SELECT *
FROM read_parquet('C:\Users\Dell\OneDrive\Documentos\GitHub\ML-ETTJ26\data\03_refined\bcb\demab\demab_government_bonds_secondary_market_refined.parquet');

CREATE OR REPLACE VIEW vw_refined_bcb_demab_ltn AS
SELECT *
FROM vw_refined_bcb_demab_government_bonds_secondary_market
WHERE sigla = 'LTN';

CREATE OR REPLACE VIEW vw_refined_bcb_demab_ntnf AS
SELECT *
FROM vw_refined_bcb_demab_government_bonds_secondary_market
WHERE sigla = 'NTN-F';

COPY (
    SELECT *
    FROM vw_refined_bcb_demab_government_bonds_secondary_market)
    TO 'data/03_refined/bcb/demab'
        (
        FORMAT PARQUET,
        PARTITION_BY (sigla),
        OVERWRITE_OR_IGNORE TRUE
    );