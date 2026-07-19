from datetime import date

import duckdb
import pandas as pd
import pytest

from ml_ettj26.pipelines.refined.b3_di1.nodes import (
    BUILD_B3_DI1_REFINED_QUERY,
)


def test_refined_di1_query_keeps_only_valid_contracts_and_sanitizes_quotes():
    connection = duckdb.connect()
    connection.execute("SET TimeZone = 'America/Sao_Paulo'")
    connection.execute(
        """
        CREATE TABLE vw_trusted_b3_forwards_metadata (
            TckrSymb VARCHAR,
            asset VARCHAR,
            maturity_date TIMESTAMPTZ
        );

        INSERT INTO vw_trusted_b3_forwards_metadata VALUES
            ('DI1N24', 'DI1', TIMESTAMPTZ '2024-07-01 00:00:00+00'),
            ('DI1M24', 'DI1', TIMESTAMPTZ '2024-06-10 00:00:00+00'),
            ('DI1Q24', 'DI1', TIMESTAMPTZ '2024-08-01 00:00:00+00');

        CREATE TABLE vw_trusted_b3_forwards_di1_quotes (
            TradDt TIMESTAMPTZ,
            TckrSymb VARCHAR,
            snapshot_ts_utc TIMESTAMPTZ,
            ingestion_ts_utc TIMESTAMPTZ,
            AdjstdQtTax DOUBLE,
            AdjstdQt DOUBLE,
            BestBidPric DOUBLE,
            BestAskPric DOUBLE,
            LastPric DOUBLE,
            TradAvrgPric DOUBLE,
            MinPric DOUBLE,
            MaxPric DOUBLE,
            TradQty DOUBLE
        );

        INSERT INTO vw_trusted_b3_forwards_di1_quotes VALUES
            (
                TIMESTAMPTZ '2024-06-10 00:00:00+00',
                'DI1N24',
                TIMESTAMPTZ '2024-06-10 18:00:00+00',
                TIMESTAMPTZ '2024-06-10 18:01:00+00',
                10.10, 99000, 10.20, 10.30, 10.20, 10.20, 10.00, 10.50, 100
            ),
            (
                TIMESTAMPTZ '2024-06-10 00:00:00+00',
                'DI1N24',
                TIMESTAMPTZ '2024-06-10 20:00:00+00',
                TIMESTAMPTZ '2024-06-10 20:01:00+00',
                10.20, 98900, 10.40, 10.30, 10.60, 10.25, 10.00, 10.50, 200
            ),
            (
                TIMESTAMPTZ '2024-06-10 00:00:00+00',
                'DI1M24',
                TIMESTAMPTZ '2024-06-10 20:00:00+00',
                TIMESTAMPTZ '2024-06-10 20:01:00+00',
                10.00, 100000, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            ),
            (
                TIMESTAMPTZ '2024-06-10 00:00:00+00',
                'DI1Q24',
                TIMESTAMPTZ '2024-06-10 20:00:00+00',
                TIMESTAMPTZ '2024-06-10 20:01:00+00',
                10.30, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL
            );

        CREATE TABLE vw_refined_calendar_br (
            date DATE,
            is_business_day BOOLEAN,
            bd_index BIGINT
        );

        INSERT INTO vw_refined_calendar_br VALUES
            (DATE '2024-06-10', TRUE, 100),
            (DATE '2024-07-01', TRUE, 115),
            (DATE '2024-08-01', TRUE, 138);
        """
    )

    result = connection.execute(BUILD_B3_DI1_REFINED_QUERY).fetchdf()

    assert len(result) == 1
    assert result.iloc[0]["date"] == pd.Timestamp(date(2024, 6, 10))
    assert result.iloc[0]["ticker"] == "DI1N24"
    assert result.iloc[0]["adjusted_price"] == pytest.approx(10.20)
    assert result.iloc[0]["bd_to_maturity"] == 15
    assert pd.isna(result.iloc[0]["bid_price"])
    assert pd.isna(result.iloc[0]["ask_price"])
    assert pd.isna(result.iloc[0]["close_price"])
    assert result.iloc[0]["average_price"] == pytest.approx(10.25)
    assert result.iloc[0]["minimum_price"] == pytest.approx(10.00)
    assert result.iloc[0]["maximum_price"] == pytest.approx(10.50)
    assert result.iloc[0]["quantity"] == pytest.approx(200)
