from __future__ import annotations

import pandas as pd

from ml_ettj26.pipelines.trusted.b3.PriceReport.DI1.nodes import build_b3_di1_range_node


def test_build_b3_di1_range_node_partitions_outputs_daily(monkeypatch):
    quotes_df = pd.DataFrame(
        {
            "TradDt": pd.to_datetime(
                ["2021-01-04T00:00:00Z", "2021-01-04T00:00:00Z", "2021-01-05T00:00:00Z"],
                utc=True,
            ),
            "TckrSymb": ["DI1F21", "DI1G21", "DI1F21"],
            "lineage_id": ["lin-1", "lin-1", "lin-2"],
        }
    )
    lineage_df = pd.DataFrame(
        {
            "lineage_id": ["lin-1", "lin-2"],
            "outer_zip": ["PR210104_20210104.zip", "PR210105_20210105.zip"],
        }
    )
    instrument_master_df = pd.DataFrame({"TckrSymb": ["DI1F21", "DI1G21"]})

    def fake_build_b3_di1_trusted_month(**kwargs):
        return quotes_df, lineage_df, instrument_master_df

    monkeypatch.setattr(
        "ml_ettj26.pipelines.trusted.b3.PriceReport.DI1.nodes.build_b3_di1_trusted_month",
        fake_build_b3_di1_trusted_month,
    )

    quotes_daily, lineage_daily, instr_df = build_b3_di1_range_node(
        raw_b3_price_report_zip_paths=["data/01_raw/b3/PriceReport/PR210104_20210104.zip"],
        trusted_ref_calendar_bd_index=pd.DataFrame(),
        trusted_b3_di1_instrument_master=pd.DataFrame(),
        b3_di1_range={"start_month": "2021-01", "end_month": "2021-01"},
        b3_price_report={"head_bytes": 1024},
    )

    assert sorted(quotes_daily) == ["2021-01-04", "2021-01-05"]
    assert sorted(lineage_daily) == ["2021-01-04", "2021-01-05"]
    assert list(quotes_daily["2021-01-04"]["TckrSymb"]) == ["DI1F21", "DI1G21"]
    assert list(lineage_daily["2021-01-04"]["lineage_id"]) == ["lin-1"]
    assert list(lineage_daily["2021-01-05"]["lineage_id"]) == ["lin-2"]
    assert instr_df.equals(instrument_master_df)
