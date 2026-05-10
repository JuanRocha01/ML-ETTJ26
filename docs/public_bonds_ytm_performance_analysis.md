# Análise Comparativa de Performance dos Pipelines de YTM

## Escopo

Esta análise compara três abordagens de processamento de YTM para uma amostra de 5.000 observações de títulos públicos:

1. `public_bonds_mart`: pipeline original.
2. `public_bonds_mart_batch`: pipeline com solver batch, mas ainda reconstruindo cashflows por linha.
3. `public_bonds_mart_dimension_batch`: pipeline com pré-processamento de cashflows por ISIN e cálculo batch a partir da dimensão.

Os resultados abaixo foram produzidos pelos scripts:

```text
src/scripts/profile_public_bonds_mart.py
src/scripts/profile_public_bonds_mart_batch.py
src/scripts/profile_public_bonds_mart_dimension_batch.py
```

## Resumo Executivo

O pipeline baseado em dimensão de cashflows é muito mais rápido. Para 5.000 observações:

```text
Original:              238.06s end-to-end
Solver batch:          228.36s end-to-end
Dimensão + batch:       10.44s end-to-end
```

Em throughput:

```text
Original:              21.00 bonds/s
Solver batch:          21.90 bonds/s
Dimensão + batch:     478.88 bonds/s
```

O ganho relevante veio da remoção da reconstrução repetida de contratos, schedules, cashflows e year fractions no loop histórico. O solver batch sozinho não resolveu o gargalo, porque o custo principal estava antes do solver.

## Comparação End-to-End

| Pipeline | Tempo total | Bonds/s end-to-end | Observações | Falhas |
|---|---:|---:|---:|---:|
| Original | 238.06s | 21.00 | 5.000 | 0 |
| Solver batch | 228.36s | 21.90 | 5.000 | 0 |
| Dimensão + batch | 10.44s | 478.88 | 5.000 | 0 |

O pipeline `public_bonds_mart_dimension_batch` foi aproximadamente:

```text
22.8x mais rápido que o original
21.9x mais rápido que o solver batch
```

## Comparação do Cálculo do Mart

| Pipeline | Tempo de cálculo do mart | Bonds/s no cálculo |
|---|---:|---:|
| Original | 236.77s | 21.12 |
| Solver batch | 226.75s | 22.05 |
| Dimensão + batch | 4.12s | 1.213,16 |

O cálculo do mart no pipeline com dimensão atinge cerca de **1.213 bonds/s**, contra cerca de **21-22 bonds/s** nos fluxos anteriores.

Isso confirma que a principal melhoria vem da troca de:

```text
contrato + schedule + cashflow + calendário por linha
```

por:

```text
lookup de arrays por ISIN + filtro por bd_index
```

## Pipeline Original

Script:

```powershell
uv run python src/scripts/profile_public_bonds_mart.py --limit 5000
```

Tempos principais:

```text
row_loop_compute_yields_and_durations: 236.77s, 99.46%
total_timed_seconds:                   238.06s
```

Throughput:

```text
mart_calculation_bonds_per_second: 21.12
end_to_end_bonds_per_second:      21.00
```

Distribuição por tipo:

```text
LTN:PRICE:   3.013 observações, 22.43s, 7.44 ms/linha
NTN-F:PRICE: 1.987 observações, 213.54s, 107.47 ms/linha
```

Métodos de solver:

```text
ZERO_COUPON: 3.013
NEWTON:      1.987
```

Diagnóstico:

O gargalo do pipeline original está quase inteiro no loop de cálculo. A NTN-F é o ponto mais caro: embora represente cerca de 40% das linhas, responde por mais de 90% do tempo do loop.

Motivo:

- reconstrução de `NTNFContract`;
- geração de schedule;
- geração de múltiplos cashflows;
- chamadas repetidas a calendário;
- cálculo de duration por cashflow.

## Pipeline com Solver Batch

Script:

```powershell
uv run python src/scripts/profile_public_bonds_mart_batch.py --limit 5000
```

Tempos principais:

```text
prepare_rows_cashflows_and_problems: 218.78s, 95.81%
yield_to_maturity_batch:               0.31s, 0.14%
finalize_success_rows_and_durations:   7.66s, 3.35%
total_timed_seconds:                 228.36s
```

Throughput:

```text
mart_calculation_bonds_per_second: 22.05
solver_problems_per_second:    16.071,75
end_to_end_bonds_per_second:      21.90
```

Métodos:

```text
ZERO_COUPON:   3.013
NEWTON_BATCH:  1.987
```

Diagnóstico:

O solver batch funcionou: resolveu 5.000 problemas em 0,31s, com mais de 16 mil problemas por segundo. Porém o tempo total quase não caiu, porque a etapa dominante continuou sendo a preparação dos problemas.

O profiling de memória também aponta o mesmo problema: as maiores alocações estão em componentes de cashflow, principal, interest, `YieldProblem` e objetos de calendário.

Conclusão:

O solver deixou de ser gargalo. A preparação dos cashflows passou a ser explicitamente o ponto crítico.

## Pipeline com Dimensão de Cashflows + Batch

Script:

```powershell
uv run python src/scripts/profile_public_bonds_mart_dimension_batch.py --limit 5000
```

Tempos principais:

```text
cashflow_build_dimension:        3.94s, 37.70%
mart_dimension_batch_calculation: 4.12s, 39.47%
total_timed_seconds:            10.44s
```

Throughput:

```text
mart_calculation_bonds_per_second: 1.213,16
end_to_end_bonds_per_second:        478,88
```

Dimensão gerada:

```text
instrumentos: 113
LTN:          88
NTN-F:        25
cashflows:   480
INTEREST:    367
PRINCIPAL:   113
max cashflows por ISIN: 23
```

Métodos:

```text
ZERO_COUPON:   3.050
NEWTON_BATCH:  1.950
```

Memória:

```text
cashflow_dimension_memory_mb: 0.16 MB
output_memory_mb:             1.63 MB
peak_mb:                     41.25 MB
```

Diagnóstico:

O pré-processamento de cashflows é pequeno e barato porque só existem 113 instrumentos distintos. A dimensão final tem apenas 480 linhas e ocupa cerca de 0,16 MB.

O cálculo histórico fica rápido porque cada observação passa a fazer:

```text
lookup por ISIN
filtro por ref_bd_index
montagem de pares (t, amount)
solver batch
duration a partir dos pares
```

Em vez de:

```text
criar contrato
criar schedule
criar cashflows
consultar calendário por cashflow
montar problema
resolver solver
calcular duration recalculando year_fraction
```

## Por Que Houve Ganho

O ganho não veio principalmente de trocar Newton por Newton batch. Isso melhorou a parte numérica, mas a parte numérica já era pequena.

O ganho veio de mudar a arquitetura:

```text
Antes:
  cada linha histórica reconstrói a estrutura financeira do instrumento

Depois:
  cada ISIN tem sua estrutura financeira pré-computada uma vez
  cada linha histórica apenas filtra os fluxos futuros por bd_index
```

Com poucos ISINs e muitas observações históricas, essa troca é altamente favorável.

## Gargalos Restantes

No pipeline dimensão + batch, os maiores blocos são:

```text
cashflow_build_dimension:         3.94s
mart_dimension_batch_calculation: 4.12s
mart_load_curve_candidates:       0.90s
cashflow_load_calendar:           0.56s
```

Possíveis próximos ganhos:

1. Persistir e reutilizar a dimensão de cashflows em vez de reconstruí-la em toda comparação.
2. Evitar carregar o calendário duas vezes no script diagnóstico.
3. Adicionar `ref_bd_index` diretamente na view de candidatos para eliminar lookup de calendário no node.
4. Reduzir overhead de criação de `YieldProblem` por linha, fazendo o batch solver aceitar arrays de `times`, `amounts` e `prices` diretamente.
5. Desativar `tqdm` em execuções de produção se o overhead aparecer relevante em volumes maiores.

## Conclusão

A comparação mostra que a estratégia correta é:

```text
Engine de contratos para gerar dimensão estática por ISIN
+ cálculo histórico em arrays filtrados por bd_index
+ solver batch
```

Essa abordagem preserva a coerência da engine e remove o gargalo real. Para a amostra de 5.000 linhas, o tempo caiu de cerca de 238s para 10s, com o throughput saindo de 21 bonds/s para quase 479 bonds/s end-to-end.
