from __future__ import annotations

import duckdb

from ml_ettj26.infraestructure.duckdb.connections import get_connection
from ml_ettj26.infraestructure.duckdb.runners import run_sql_file

def main():
    con = get_connection()

    try:
        record_refined_calendar(con)
        record_refined_bcb_sgs(con)
        record_refined_bcb_demab(con)

    finally:
        con.close()

def record_refined_calendar(con: duckdb.DuckDBPyConnection) -> None:
    run_sql_file(con, "sql/03_refined/calendar.sql")

def record_refined_bcb_sgs(con: duckdb.DuckDBPyConnection) -> None:
    run_sql_file(con, "sql/03_refined/bcb_sgs.sql")

def record_refined_bcb_demab(con: duckdb.DuckDBPyConnection) -> None:
    run_sql_file(con, "sql/03_refined/bcb_demab.sql")

if __name__ == "__main__":
    main()
