from datetime import date

from ml_ettj26.extractors.b3_pregao_specs import PRICE_REPORT, SWAP_MARKET_RATES


def test_price_report_file_code_and_filelist():
    d = date(2021, 3, 2)
    assert PRICE_REPORT.build_file_code(d) == "PR210302.zip"
    assert PRICE_REPORT.build_filelist_param(d) == "PR210302.zip,"


def test_swap_market_rates_file_code_and_filelist():
    d = date(2020, 2, 13)
    assert SWAP_MARKET_RATES.build_file_code(d) == "TS200213.ex_"
    assert SWAP_MARKET_RATES.build_filelist_param(d) == "TS200213.ex_,"


def test_saved_filename_format_nomeoriginal_yyyymmdd_zip():
    d = date(2020, 2, 13)
    # stem do "nome original lógico" => TS200213
    assert SWAP_MARKET_RATES.build_saved_filename(d) == "TS200213_20200213.zip"

    d2 = date(2021, 3, 2)
    assert PRICE_REPORT.build_saved_filename(d2) == "PR210302_20210302.zip"
