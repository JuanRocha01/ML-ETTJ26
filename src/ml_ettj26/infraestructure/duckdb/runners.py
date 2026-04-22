from __future__ import annotations

import duckdb
from pathlib import Path

def run_sql_file(con: duckdb.DuckDBPyConnection, file_path: str) -> None:
    print(f"Running {file_path}")
    sql_script = Path(file_path).read_text(encoding="utf-8")
    con.execute(sql_script)
