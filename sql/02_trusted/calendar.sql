CREATE OR REPLACE VIEW vw_trusted_calendar_br AS
SELECT * 
FROM read_parquet('C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/02_trusted/ref/calendar_bd_index.parquet');

CREATE OR REPLACE VIEW vw_trusted_holidays_br AS
SELECT * 
FROM read_parquet('C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/02_trusted/ref/anbima_holidays.parquet');
