from .connections import get_connection

def register_views():
    con = get_connection()

    con.execute("""
    CREATE OR REPLACE VIEW vw_ref_br_calendar AS
    SELECT *
    FROM 'C:/Users/Dell/OneDrive/Documentos/GitHub/ML-ETTJ26/data/calendars/03_refined/Calendar_BR_dim.parquet'
    """)

    con.close()
    