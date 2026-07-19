from datetime import date
from pathlib import Path

import duckdb


SQL_PATH = (
    Path(__file__).resolve().parents[1]
    / "sql"
    / "marts"
    / "public_bonds"
    / "02_mart_public_bonds_curve_candidates_and_exclusions.sql"
)


def test_tenor_occupancy_flag_distinguishes_good_medium_and_poor_days():
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE mart_public_bonds_quotes_quality (
            ref_date DATE,
            bd_to_maturity INTEGER,
            eligible_for_curve_input BOOLEAN
        )
        """
    )
    connection.executemany(
        "INSERT INTO mart_public_bonds_quotes_quality VALUES (?, ?, TRUE)",
        [
            # GOOD: 2 curtos, 4 médios e 4 longos.
            ("2024-01-02", 100),
            ("2024-01-02", 180),
            ("2024-01-02", 300),
            ("2024-01-02", 450),
            ("2024-01-02", 600),
            ("2024-01-02", 800),
            ("2024-01-02", 1000),
            ("2024-01-02", 1100),
            ("2024-01-02", 1200),
            ("2024-01-02", 1300),
            # MEDIUM: há ocupação nas três regiões, mas pouca densidade.
            ("2024-01-03", 100),
            ("2024-01-03", 500),
            ("2024-01-03", 1000),
            # POOR: não há título no longo prazo.
            ("2024-01-04", 100),
            ("2024-01-04", 200),
            ("2024-01-04", 500),
        ],
    )
    connection.execute(SQL_PATH.read_text(encoding="utf-8"))

    daily_flags = connection.execute(
        """
        SELECT
            ref_date,
            ANY_VALUE(numero_observacoes_curto) AS curto,
            ANY_VALUE(numero_observacoes_medio) AS medio,
            ANY_VALUE(numero_observacoes_longo) AS longo,
            ANY_VALUE(flag_ocupacao_tenors) AS flag
        FROM mart_public_bonds_curve_candidates
        GROUP BY ref_date
        ORDER BY ref_date
        """
    ).fetchall()

    assert daily_flags == [
        (date(2024, 1, 2), 2, 4, 4, "GOOD"),
        (date(2024, 1, 3), 1, 1, 1, "MEDIUM"),
        (date(2024, 1, 4), 2, 1, 0, "POOR"),
    ]
