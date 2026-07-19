import matplotlib.pyplot as plt
import pandas as pd
import pytest

from ml_ettj26.analytics.public_bonds_quality import (
    plotar_qualidade_maxima_mensal,
    verificar_qualidade_maxima_mensal,
)


def test_verificar_qualidade_maxima_mensal_resumes_each_available_month():
    mart = pd.DataFrame(
        [
            {
                "ref_date": "2024-01-02",
                "flag_volume": "HIGH",
                "flag_cobertura_tenors": "GOOD",
                "flag_ocupacao_tenors": "GOOD",
            },
            {
                # Segunda observação do mesmo dia no mart.
                "ref_date": "2024-01-02",
                "flag_volume": "HIGH",
                "flag_cobertura_tenors": "GOOD",
                "flag_ocupacao_tenors": "GOOD",
            },
            {
                "ref_date": "2024-02-01",
                "flag_volume": "HIGH",
                "flag_cobertura_tenors": "GOOD",
                "flag_ocupacao_tenors": "MEDIUM",
            },
            {
                "ref_date": "2025-01-03",
                "flag_volume": "HIGH",
                "flag_cobertura_tenors": "GOOD",
                "flag_ocupacao_tenors": "GOOD",
            },
        ]
    )

    result = verificar_qualidade_maxima_mensal(mart)

    assert result["mes"].astype(str).tolist() == ["2024-01", "2024-02", "2025-01"]
    assert result["dias_qualidade_maxima"].tolist() == [1, 0, 1]
    assert result["mes_aprovado"].tolist() == [True, False, True]


def test_plotar_qualidade_maxima_mensal_returns_year_month_map():
    resumo = pd.DataFrame(
        {
            "mes": pd.PeriodIndex(["2024-01", "2024-02", "2025-01"], freq="M"),
            "dias_qualidade_maxima": [2, 0, 1],
            "mes_aprovado": [True, False, True],
        }
    )

    ax = plotar_qualidade_maxima_mensal(resumo)

    assert ax.get_xlabel() == "Mês"
    assert ax.get_ylabel() == "Ano"
    assert ax.images[0].get_array().shape == (2, 12)
    plt.close(ax.figure)


def test_verificar_qualidade_maxima_mensal_rejects_inconsistent_daily_flags():
    mart = pd.DataFrame(
        {
            "ref_date": ["2024-01-02", "2024-01-02"],
            "flag_volume": ["HIGH", "LOW"],
            "flag_cobertura_tenors": ["GOOD", "GOOD"],
            "flag_ocupacao_tenors": ["GOOD", "GOOD"],
        }
    )

    with pytest.raises(ValueError, match="Flags inconsistentes"):
        verificar_qualidade_maxima_mensal(mart)
