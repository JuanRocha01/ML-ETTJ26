from __future__ import annotations

from pathlib import Path

import pandas as pd

from ml_ettj26.io.datasets.safe_parquet import SafeParquetDataset


def test_safe_parquet_dataset_falls_back_when_primary_is_empty(tmp_path: Path):
    primary = tmp_path / "primary.parquet"
    fallback = tmp_path / "fallback.parquet"

    primary.write_bytes(b"")
    expected = pd.DataFrame({"TckrSymb": ["DI1F21"]})
    expected.to_parquet(fallback, index=False)

    dataset = SafeParquetDataset(
        filepath=str(primary),
        fallback_filepath=str(fallback),
    )

    loaded = dataset.load()

    assert loaded.equals(expected)


def test_safe_parquet_dataset_returns_empty_df_when_no_candidate_is_valid(tmp_path: Path):
    dataset = SafeParquetDataset(
        filepath=str(tmp_path / "missing.parquet"),
        fallback_filepath=str(tmp_path / "also_missing.parquet"),
    )

    loaded = dataset.load()

    assert loaded.empty
