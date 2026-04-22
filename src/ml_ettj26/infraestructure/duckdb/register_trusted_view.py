from __future__ import annotations

from pathlib import Path
import duckdb

from ml_ettj26.infraestructure.duckdb.connections import get_connection
from ml_ettj26.infraestructure.duckdb.runners import run_sql_file

def main():
    con = get_connection()

    try:
        record_trusted_calendar(con)
        record_trusted_sgs(con)
        record_trusted_demab(con)
        record_trusted_forwards_di1(con)
        record_trusted_swap_dixpre(con)
    finally:
        con.close()

def record_trusted_calendar(con: duckdb.DuckDBPyConnection) -> None:
    run_sql_file(con, "sql/02_trusted/calendar.sql")

def record_trusted_sgs(con: duckdb.DuckDBPyConnection) -> None:
    run_sql_file(con, "sql/02_trusted/bcb_sgs.sql")

def record_trusted_demab(con: duckdb.DuckDBPyConnection) -> None:
    run_sql_file(con, "sql/02_trusted/bcb_demab.sql")

def record_trusted_forwards_di1(con: duckdb.DuckDBPyConnection) -> None:
    run_sql_file(con, "sql/02_trusted/b3_forwards_di1.sql")

def record_trusted_swap_dixpre(con: duckdb.DuckDBPyConnection) -> None:
    run_sql_file(con, "sql/02_trusted/b3_swaps_dixpre.sql")

if __name__ == "__main__":
    main()
