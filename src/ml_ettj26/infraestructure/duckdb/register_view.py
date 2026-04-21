from .connections import get_connection

def register_views_calendar_BR():
    con = get_connection()

    con.execute("""
    CREATE OR REPLACE VIEW vw_ref_br_calendar AS
    SELECT *
    FROM 'C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/calendars/03_refined/Calendar_BR_dim.parquet'
    """)

    path_sgs_meta = "C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/02_trusted/bcb/sgs/series_meta.parquet"
    path_sgs_points = "C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/02_trusted/bcb/sgs/points.parquet"

    con.execute(f"""
    CREATE OR REPLACE VIEW vw_sgs_selic AS
    WITH
    
    meta AS (
        SELECT *
        FROM read_parquet('{path_sgs_meta}')
        WHERE series_id = 432
    ),

    points AS (
        SELECT series_id, ref_date, value, raw_hash
        FROM read_parquet('{path_sgs_points}')
        WHERE series_id = 432
    )

    SELECT p.ref_date AS date,
           m.series_id,
           m.series_name,
           p.value,
           m.unit,
           m.frequency,
           m.source,
           p.raw_hash
    FROM meta AS m
    JOIN points AS p
    USING (series_id)
    ORDER BY ref_date
    """)

    con.execute(f"""
    CREATE OR REPLACE VIEW vw_sgs_ipca AS
    WITH
    
    meta AS (
        SELECT *
        FROM read_parquet('{path_sgs_meta}')
        WHERE series_id = 433
    ),

    points AS (
        SELECT series_id, ref_date, value, raw_hash
        FROM read_parquet('{path_sgs_points}')
        WHERE series_id = 433
    )

    SELECT p.ref_date AS date,
           m.series_id,
           m.series_name,
           p.value,
           m.unit,
           m.frequency,
           m.source,
           p.raw_hash
    FROM meta AS m
    JOIN points AS p
    USING (series_id)
    ORDER BY ref_date
    """)    

    con.close()

    return
