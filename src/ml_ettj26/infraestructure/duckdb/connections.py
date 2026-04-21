import duckdb
from pathlib import Path

def get_connection():
    db_path = Path(r"data\duckdb\ettj26.duckdb")
    return duckdb.connect(db_path)
