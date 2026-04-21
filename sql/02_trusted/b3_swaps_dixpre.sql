CREATE OR REPLACE VIEW vw_trusted_b3_swaps_metadata AS
SELECT *
FROM read_parquet('C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/02_trusted/b3/swaps/swap_master.parquet');

CREATE OR REPLACE VIEW vw_trusted_b3_swaps_lineage AS
SELECT *
FROM read_parquet('C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/02_trusted/b3/swaps/data_lineage.parquet');

CREATE OR REPLACE VIEW vw_trusted_b3_swaps_dixpre_quotes AS
SELECT *
FROM read_parquet('C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/02_trusted/b3/swaps/swap_dixpre/*.parquet');
