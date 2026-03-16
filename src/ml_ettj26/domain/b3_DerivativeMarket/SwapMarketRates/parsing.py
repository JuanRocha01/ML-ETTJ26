from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SwapRawRecord:
    seq_linha: str
    campo_constante_1: str
    data_ref_raw: str
    codigo_produto: str
    nome_produto: str
    dias_entrega: str
    dias_uteis: str
    sinal: str
    valor_raw: str
    tipo_cotacao: str
    dias_corridos_maturity: str

def parse_swap_txt_line(line: str) -> SwapRawRecord:
    return SwapRawRecord(
        seq_linha=line[0:6],
        campo_constante_1=line[6:11],
        data_ref_raw=line[11:19],
        codigo_produto=line[19:24].strip(),
        nome_produto=line[26:41].strip(),
        dias_entrega=line[41:46].strip(),
        dias_uteis=line[46:51].strip(),
        sinal=line[51:52].strip(),
        valor_raw=line[52:66].strip(),
        tipo_cotacao=line[66:67].strip(),
        dias_corridos_maturity=line[67:72].strip(),
    )
