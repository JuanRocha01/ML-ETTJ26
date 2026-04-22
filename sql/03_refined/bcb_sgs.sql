CREATE OR REPLACE VIEW vw_refined_selic AS
SELECT *
FROM read_parquet('C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/03_refined/bcb/sgs/selic.parquet');
                    
CREATE OR REPLACE VIEW vw_refined_ipca AS
SELECT *
FROM read_parquet('C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/03_refined/bcb/sgs/ipca.parquet');