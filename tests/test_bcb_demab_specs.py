from ml_ettj26.extractors.bcb_demab_specs import DEMAB_NEGOCIACOES

#-----------------------------------TESTE 1 -----------------------------------
def test_demab_spec_builds_relative_url_and_filename():
    yyyymm = "202401"

    rel = DEMAB_NEGOCIACOES.build_relative_url(tipo="T", yyyymm=yyyymm)
    fname = DEMAB_NEGOCIACOES.build_filename(tipo="T", yyyymm=yyyymm)

    assert rel == "negociacoes/download/NegT202401.ZIP"
    assert fname == "NegT202401.ZIP"
