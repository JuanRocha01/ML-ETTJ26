from __future__ import annotations

from pathlib import Path

from ml_ettj26.infraestructure.duckdb.connections import get_connection
from ml_ettj26.infraestructure.duckdb.runners import run_sql_file


def register_duckdb_views(sql_files: list[str]) -> None:
    """
    Register multiple DuckDB views from SQL files.
    """
    con = get_connection()

    try:
        for sql_file in sql_files:
            run_sql_file(con, Path(sql_file))
    finally:
        con.close()