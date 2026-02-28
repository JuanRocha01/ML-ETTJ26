from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pandas as pd

from ml_ettj26.domain.b3_PriceReport.service import build_b3_di1_trusted_month


def _make_outer_zip_with_inner_xmls(outer_zip_path: Path, inner_zip_name: str, xml_map: dict[str, str]) -> None:
    inner_buf = io.BytesIO()
    with zipfile.ZipFile(inner_buf, "w", compression=zipfile.ZIP_DEFLATED) as inner:
        for xml_name, xml_text in xml_map.items():
            inner.writestr(xml_name, xml_text.encode("utf-8"))

    with zipfile.ZipFile(outer_zip_path, "w", compression=zipfile.ZIP_DEFLATED) as outer:
        outer.writestr(inner_zip_name, inner_buf.getvalue())


def _valid_di1_xml(ticker: str = "DI1F21") -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<BizData xmlns="urn:bvmf.217.01.xsd">
  <AppHdr>
    <CreDt>2021-01-04T23:30:00Z</CreDt>
  </AppHdr>
  <Document>
    <PricRpt>
      <TradDt><Dt>2021-01-04</Dt></TradDt>
      <SctyId><TckrSymb>{ticker}</TckrSymb></SctyId>
      <TradDtls><TradQty>10</TradQty></TradDtls>
      <FinInstrmAttrbts>
        <AdjstdQtTax>1.23</AdjstdQtTax>
        <AdjstdQt>1000</AdjstdQt>
      </FinInstrmAttrbts>
    </PricRpt>
  </Document>
</BizData>
"""


def _invalid_xml_with_mismatched_tag(ticker: str = "DI1F21") -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<BizData xmlns="urn:bvmf.217.01.xsd">
  <AppHdr>
    <CreDt>2021-01-04T23:31:00Z</CreDt>
  </AppHdr>
  <Document>
    <PricRpt>
      <TradDt><Dt>2021-01-04</Dt></TradDt>
      <SctyId><TckrSymb>{ticker}</TckrSymb></SctyId>
      <FinInstrmAttrbts>
        <AdjstdQtTax>1.23</AdjstdQtTax>
        <AdjstdQt>1000/AdjstdQt>
      </FinInstrmAttrbts>
    </PricRpt>
  </Document>
</BizData>
"""


def _bd_index_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2021-01-04"], utc=True),
            "is_business_day": [True],
        }
    )


def test_fallback_uses_next_parseable_xml_when_latest_is_invalid(tmp_path: Path, caplog):
    outer = tmp_path / "PR210104_20210104.zip"
    _make_outer_zip_with_inner_xmls(
        outer,
        inner_zip_name="PR210104.zip",
        xml_map={
            "BVBG.086.01_file_2.xml": _invalid_xml_with_mismatched_tag(),
            "BVBG.086.01_file_1.xml": _valid_di1_xml(),
        },
    )

    caplog.set_level("INFO")
    quotes_df, lineage_df, instr_df = build_b3_di1_trusted_month(
        raw_zip_paths=[str(outer)],
        bd_index_df=_bd_index_df(),
        year=2021,
        month=1,
        previous_instrument_master_df=pd.DataFrame(),
    )

    assert len(quotes_df) == 1
    assert quotes_df.iloc[0]["TckrSymb"] == "DI1F21"
    assert len(lineage_df) == 1
    assert lineage_df.iloc[0]["xml_name"] == "BVBG.086.01_file_1.xml"
    assert len(instr_df) == 1
    assert "XML parse falhou; tentando fallback" in caplog.text
    assert "Fallback aplicado: selecionado XML parseavel mais antigo" in caplog.text


def test_when_no_parseable_xml_logs_skipped_day_and_continues(tmp_path: Path, caplog):
    bad_outer = tmp_path / "PR210104_20210104.zip"
    good_outer = tmp_path / "PR210105_20210105.zip"

    _make_outer_zip_with_inner_xmls(
        bad_outer,
        inner_zip_name="PR210104.zip",
        xml_map={"BVBG.086.01_file_2.xml": _invalid_xml_with_mismatched_tag()},
    )
    _make_outer_zip_with_inner_xmls(
        good_outer,
        inner_zip_name="PR210105.zip",
        xml_map={"BVBG.086.01_file_1.xml": _valid_di1_xml()},
    )

    caplog.set_level("WARNING")
    quotes_df, lineage_df, instr_df = build_b3_di1_trusted_month(
        raw_zip_paths=[str(bad_outer), str(good_outer)],
        bd_index_df=_bd_index_df(),
        year=2021,
        month=1,
        previous_instrument_master_df=pd.DataFrame(),
    )

    assert len(quotes_df) == 1
    assert len(lineage_df) == 1
    assert len(instr_df) == 1
    assert "Data pulada: nenhum XML parseavel no zip" in caplog.text
    assert "day=20210104" in caplog.text
