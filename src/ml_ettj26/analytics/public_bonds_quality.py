from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from matplotlib.colors import ListedColormap
from matplotlib.patches import Patch


QUALITY_FLAG_COLUMNS = [
    "flag_volume",
    "flag_cobertura_tenors",
    "flag_ocupacao_tenors",
]


def verificar_qualidade_maxima_mensal(mart: pd.DataFrame) -> pd.DataFrame:
    """Verifica se cada mês tem ao menos um dia com HIGH/GOOD/GOOD."""
    required_columns = {"ref_date", *QUALITY_FLAG_COLUMNS}
    missing_columns = sorted(required_columns.difference(mart.columns))
    if missing_columns:
        raise ValueError(f"Colunas ausentes no mart: {missing_columns}")
    if mart.empty:
        raise ValueError("O mart está vazio.")

    daily = mart.loc[:, ["ref_date", *QUALITY_FLAG_COLUMNS]].copy()
    daily["ref_date"] = pd.to_datetime(daily["ref_date"], errors="raise").dt.normalize()
    daily = daily.drop_duplicates()

    inconsistent_dates = daily.loc[
        daily["ref_date"].duplicated(keep=False), "ref_date"
    ].dt.strftime("%Y-%m-%d")
    if not inconsistent_dates.empty:
        dates = sorted(inconsistent_dates.unique().tolist())
        raise ValueError(f"Flags inconsistentes para as datas: {dates}")

    daily["qualidade_maxima"] = (
        daily["flag_volume"].eq("HIGH")
        & daily["flag_cobertura_tenors"].eq("GOOD")
        & daily["flag_ocupacao_tenors"].eq("GOOD")
    )
    daily["mes"] = daily["ref_date"].dt.to_period("M")
    daily["data_qualidade_maxima"] = daily["ref_date"].where(
        daily["qualidade_maxima"]
    )

    monthly = (
        daily.groupby("mes", observed=True)
        .agg(
            dias_disponiveis=("ref_date", "size"),
            dias_qualidade_maxima=("qualidade_maxima", "sum"),
            primeiro_dia_qualidade_maxima=("data_qualidade_maxima", "min"),
        )
        .reset_index()
    )
    monthly["mes_aprovado"] = monthly["dias_qualidade_maxima"].gt(0)

    return monthly[
        [
            "mes",
            "dias_disponiveis",
            "dias_qualidade_maxima",
            "primeiro_dia_qualidade_maxima",
            "mes_aprovado",
        ]
    ]


def plotar_qualidade_maxima_mensal(
    resumo_mensal: pd.DataFrame,
    ax: Axes | None = None,
) -> Axes:
    """Plota um mapa ano x mês dos resultados da verificação mensal."""
    required_columns = {"mes", "dias_qualidade_maxima", "mes_aprovado"}
    missing_columns = sorted(required_columns.difference(resumo_mensal.columns))
    if missing_columns:
        raise ValueError(f"Colunas ausentes no resumo mensal: {missing_columns}")
    if resumo_mensal.empty:
        raise ValueError("O resumo mensal está vazio.")

    data = resumo_mensal.copy()
    if not isinstance(data["mes"].dtype, pd.PeriodDtype):
        data["mes"] = pd.to_datetime(data["mes"], errors="raise").dt.to_period("M")
    data["ano"] = data["mes"].dt.year
    data["numero_mes"] = data["mes"].dt.month

    years = range(int(data["ano"].min()), int(data["ano"].max()) + 1)
    status = (
        data.pivot(index="ano", columns="numero_mes", values="mes_aprovado")
        .reindex(index=years, columns=range(1, 13))
    )
    good_days = (
        data.pivot(
            index="ano",
            columns="numero_mes",
            values="dias_qualidade_maxima",
        )
        .reindex(index=years, columns=range(1, 13))
    )

    if ax is None:
        _, ax = plt.subplots(figsize=(12, max(2.5, len(status.index) * 0.45)))

    values = status.astype("float").fillna(-1.0).to_numpy()
    cmap = ListedColormap(["#D9D9D9", "#D73027", "#1A9850"])
    ax.imshow(values, aspect="auto", cmap=cmap, vmin=-1, vmax=1)

    for row_index in range(values.shape[0]):
        for column_index in range(values.shape[1]):
            value = values[row_index, column_index]
            if value < 0:
                label = "–"
            elif value == 0:
                label = "×"
            else:
                label = f"✓\n{int(good_days.iloc[row_index, column_index])}d"
            ax.text(
                column_index,
                row_index,
                label,
                ha="center",
                va="center",
                color="black" if value < 0 else "white",
                fontsize=9,
            )

    ax.set_xticks(np.arange(12), labels=["Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
                                        "Jul", "Ago", "Set", "Out", "Nov", "Dez"])
    ax.set_yticks(np.arange(len(status.index)), labels=status.index)
    ax.set_xlabel("Mês")
    ax.set_ylabel("Ano")
    ax.set_title("Existência de dia com qualidade máxima (HIGH / GOOD / GOOD)")
    ax.legend(
        handles=[
            Patch(facecolor="#1A9850", label="Aprovado"),
            Patch(facecolor="#D73027", label="Sem dia de qualidade máxima"),
            Patch(facecolor="#D9D9D9", label="Sem dados"),
        ],
        loc="upper center",
        bbox_to_anchor=(0.5, -0.12),
        ncols=3,
        frameon=False,
    )
    ax.figure.tight_layout()
    return ax

