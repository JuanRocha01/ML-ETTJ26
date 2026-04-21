CREATE OR REPLACE VIEW vw_trusted_b3_forwards_metadata AS
SELECT *
FROM read_parquet('C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/02_trusted/b3/forwards/di1_instrument_master.parquet');

CREATE OR REPLACE VIEW vw_trusted_b3_forwards_di1_lineage AS
SELECT *
FROM read_parquet('C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/02_trusted/b3/forwards/di1_lineage/*.parquet');

CREATE OR REPLACE VIEW vw_trusted_b3_forwards_di1_quotes AS
SELECT *
FROM read_parquet('C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/02_trusted/b3/forwards/di1_quotes_daily/*.parquet');