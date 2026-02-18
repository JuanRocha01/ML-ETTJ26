from __future__ import annotations

import pandas as pd


def read_demab_csv(stream) -> pd.DataFrame:
    # separador ; e decimal com vírgula
    df = pd.read_csv(
        stream,
        sep=";",
        encoding="utf-8",
        decimal=",",
        dtype=str,              # lê tudo como str e normaliza depois (mais robusto)
        keep_default_na=False,  # "" fica "", e você decide o que vira NaN
    )
    return df
