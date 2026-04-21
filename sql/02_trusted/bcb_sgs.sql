CREATE OR REPLACE VIEW vw_trusted_bcb_sgs_points AS
SELECT *
FROM read_parquet('C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/02_trusted/bcb/sgs/points.parquet');

CREATE OR REPLACE VIEW vw_trusted_bcb_sgs_metadata AS
SELECT *
FROM read_parquet('C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/02_trusted/bcb/sgs/series_meta.parquet');