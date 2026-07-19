import duckdb
from pathlib import Path


def get_connection(*, read_only: bool = False):
    db_path = Path(r"data\duckdb\ml_ettj26.duckdb")
    return duckdb.connect(db_path, read_only=read_only)
