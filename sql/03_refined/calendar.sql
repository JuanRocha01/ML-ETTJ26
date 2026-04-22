CREATE OR REPLACE VIEW vw_refined_calendar_br AS
SELECT *
FROM read_parquet('C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/calendars/03_refined/Calendar_BR_dim.parquet');