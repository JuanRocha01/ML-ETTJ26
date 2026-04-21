CREATE OR REPLACE VIEW vw_trusted_bcb_demab_instruments AS
SELECT *
FROM read_parquet('C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/02_trusted/bcb/demab/instruments.parquet');

CREATE OR REPLACE VIEW vw_trusted_bcb_demab_quotes AS
SELECT *
FROM read_parquet('C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/02_trusted/bcb/demab/quotes_daily/*.parquet');