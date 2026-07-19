from datetime import date

import duckdb
import pandas as pd
import pytest

from ml_ettj26.pipelines.refined.b3_swaps.b3_dixpre.nodes import (
    BUILD_SWAP_DIPRE_REFINED_QUERY,
)


def test_refined_swap_uses_project_calendar_for_business_day_tenor():
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE vw_trusted_b3_swaps_metadata (
            CodProd VARCHAR,
            nome VARCHAR,
            underlying VARCHAR,
            fixed_leg VARCHAR,
            float_leg VARCHAR
        );

        INSERT INTO vw_trusted_b3_swaps_metadata VALUES
            ('T1PRE', 'DIxPRE', 'DI x PRE', 'PRE', 'DI');

        CREATE TABLE vw_trusted_b3_swaps_dixpre_quotes (
            TradDt TIMESTAMP,
            CodProd VARCHAR,
            days_to_maturity BIGINT,
            bd_to_maturity BIGINT,
            days_to_delivery BIGINT,
            adjusted_value DOUBLE,
            tipo_cotacao VARCHAR,
            lineage_id VARCHAR
        );

        INSERT INTO vw_trusted_b3_swaps_dixpre_quotes VALUES
            (
                TIMESTAMP '2024-06-10 00:00:00',
                'T1PRE',
                7,
                6,
                7,
                1020.0,
                'M',
                'valid'
            ),
            (
                TIMESTAMP '2024-06-14 00:00:00',
                'T1PRE',
                1,
                1,
                1,
                1010.0,
                'F',
                'weekend-zero-tenor'
            );

        CREATE TABLE vw_refined_calendar_br (
            date DATE,
            is_business_day BOOLEAN,
            bd_index BIGINT
        );

        INSERT INTO vw_refined_calendar_br VALUES
            (DATE '2024-06-10', TRUE, 100),
            (DATE '2024-06-14', TRUE, 104),
            (DATE '2024-06-15', FALSE, 104),
            (DATE '2024-06-17', TRUE, 105);
        """
    )

    result = connection.execute(BUILD_SWAP_DIPRE_REFINED_QUERY).fetchdf()

    assert len(result) == 1
    assert result.iloc[0]["date"] == pd.Timestamp(date(2024, 6, 10))
    assert result.iloc[0]["maturity"] == pd.Timestamp(date(2024, 6, 17))
    assert result.iloc[0]["days_to_maturity"] == 7
    assert result.iloc[0]["source_bd_to_maturity"] == 6
    assert result.iloc[0]["bd_to_maturity"] == 5
    assert result.iloc[0]["adjusted_value"] == pytest.approx(10.20)
