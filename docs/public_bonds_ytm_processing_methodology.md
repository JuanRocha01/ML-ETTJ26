# Metodologia de Processamento de YTM para Títulos Públicos

## Objetivo

Este documento resume as decisões técnicas tomadas para otimizar o cálculo de `yield to maturity` (YTM) dos títulos públicos no projeto. A motivação inicial foi reduzir o tempo de processamento histórico da base, que no fluxo original levava cerca de 1 hora.

A conclusão dos diagnósticos foi que o gargalo principal não estava no solver numérico em si, mas na reconstrução repetida de contratos, schedules, cashflows e frações de ano para cada observação histórica.

## Diagnóstico

Foram criados scripts de profiling para medir separadamente as etapas do pipeline:

- `src/scripts/profile_public_bonds_mart.py`: perfila o pipeline original.
- `src/scripts/profile_public_bonds_mart_batch.py`: perfila o pipeline com solver batch.

O principal resultado observado em uma amostra de 5.000 linhas foi:

```text
prepare_rows_cashflows_and_problems: 213s, cerca de 96% do tempo
yield_to_maturity_batch: 0.3s, cerca de 0.1% do tempo
finalize_success_rows_and_durations: 7.5s, cerca de 3.4% do tempo
```

Portanto, a otimização do solver reduziu parte do custo, mas não atacou o maior gargalo. O custo dominante era a preparação dos problemas: criação de `LTNContract`/`NTNFContract`, geração de schedules, cashflows, objetos `YieldProblem` e chamadas repetidas ao calendário.

## Decisões de Design

### 1. Separar problema unitário, solver unitário e solver batch

O `YieldProblem` continua representando uma observação individual. Ele foi adaptado para pré-computar e reutilizar `time_amount_pairs`, evitando recalcular cashflows futuros e year fractions em cada chamada de `objective`, `derivative` e `price_from_yield`.

O `yield_solvers.py` permanece como API pública principal:

- `yield_to_maturity(problem)`: cálculo unitário.
- `yield_to_maturity_batch(problems)`: fachada pública para cálculo em lote.

O processamento batch especializado fica em:

- `src/engine_product/pricing/yield_solvers_batch.py`

Essa separação evita misturar lógica vetorizada, máscaras de convergência e tratamento de falhas dentro do solver unitário.

### 2. Fórmula fechada para single-cashflow

Para qualquer instrumento com um único fluxo futuro positivo, a YTM é calculada por fórmula fechada:

```python
ytm = (amount / market_price) ** (1 / t) - 1
```

Essa regra é universal e não depende do tipo do título. Uma LTN é apenas um caso particular de instrumento com fluxo único.

O método retornado é `ZERO_COUPON`.

### 3. Newton em batch para múltiplos cashflows

Para instrumentos com múltiplos cashflows, foi criado o `BatchYieldSolver`, que:

1. separa problemas single-cashflow;
2. resolve single-cashflows vetorizados com NumPy;
3. agrupa problemas multi-cashflow por quantidade de fluxos;
4. aplica Newton vetorizado por grupo;
5. envia falhas individuais para fallback unitário com Brent/Newton;
6. retorna sucesso ou falha por observação, sem derrubar o lote.

Foi mantido temporariamente o método `NEWTON_BATCH` para rastreabilidade operacional. Ele não indica menor precisão; apenas registra que o cálculo foi feito no caminho vetorizado.

### 4. Preservar falhas individuais

O batch solver retorna `YieldSolverBatchResult`, que contém:

- `index`
- `result`
- `error_type`
- `error_message`
- `succeeded`

Isso permite que o node Kedro gere linhas de sucesso e linhas de falha sem interromper o processamento inteiro.

### 5. Criar dimensão estática de cashflows por ISIN

Após o profiling, ficou claro que reconstruir a estrutura de cashflows por linha era o gargalo. Como a base possui poucos ISINs distintos, foi criada uma dimensão estática:

```text
mart_public_bonds_cashflow_dimension
```

Ela é gerada uma vez por ISIN usando a própria engine de contratos, preservando a coerência com o modelo de produto.

Campos principais:

```text
isin
instrument_type
issue_date
maturity_date
cashflow_number
payment_date
payment_bd_index
issue_bd_index
bd_from_issue
cashflow_type
cashflow_type_rank
amount
accrual_start
accrual_end
notional_before
notional_after
metadata_json
```

A chave principal lógica é:

```text
isin + cashflow_number
```

Essa escolha preserva múltiplos fluxos na mesma data, como juros, amortização e principal.

### 6. Cashflows granulares, arrays agregados para pricing

A dimensão armazena os cashflows de forma granular, um componente por linha. Isso mantém a semântica da engine:

- `INTEREST`
- `AMORTIZATION`
- `PRINCIPAL`
- `FEE`
- outros tipos futuros

Para cálculo de YTM, fluxos na mesma data são agregados por `payment_bd_index`, pois compartilham o mesmo fator de desconto:

```text
interest / df + principal / df = (interest + principal) / df
```

Essa agregação acontece no adapter:

```text
src/engine_product/pricing/cashflow_arrays.py
```

### 7. Usar `bd_index` em vez de calendário no hot path

O cálculo rápido não chama mais `BU252.year_fraction` linha a linha. Em vez disso, usa:

```python
t = (payment_bd_index - ref_bd_index) / 252
```

O `payment_bd_index` vem da dimensão de cashflows e o `ref_bd_index` vem da view de calendário.

Isso remove o custo de acesso repetido ao calendário/DataFrame durante a construção dos problemas.

## Os Três Pipelines

## 1. Pipeline Original: `public_bonds_mart`

Arquivo:

```text
src/ml_ettj26/pipelines/curve_factory/public_bonds_mart/pipeline.py
```

Fluxo:

```text
SQL views
  -> carrega candidatos
  -> carrega calendário
  -> para cada linha:
       cria contrato
       gera schedule
       gera cashflows
       cria YieldProblem
       resolve YTM
       calcula duration
  -> salva mart de inputs e falhas
```

Características:

- Simples e fiel ao uso unitário da engine.
- Bom para validação e desenvolvimento.
- Lento para histórico grande, pois reconstrói tudo a cada observação.

Gargalo:

```text
contrato + schedule + cashflow + year_fraction por linha
```

## 2. Pipeline com Solver Batch: `public_bonds_mart_batch`

Arquivo:

```text
src/ml_ettj26/pipelines/curve_factory/public_bonds_mart/pipeline_batch.py
```

Fluxo:

```text
SQL views
  -> carrega candidatos
  -> carrega calendário
  -> para cada linha:
       ainda cria contrato
       ainda gera schedule
       ainda gera cashflows
       cria YieldProblem
  -> resolve todos os YieldProblem em batch
  -> calcula duration
  -> salva mart de inputs e falhas batch
```

Diferença em relação ao original:

- O solver deixa de ser chamado linha a linha.
- Single-cashflows são resolvidos com fórmula fechada vetorizada.
- Multi-cashflows são resolvidos com Newton batch agrupado por número de fluxos.
- Falhas individuais são preservadas.

Ganho observado:

- Redução parcial do tempo total.
- O solver ficou muito rápido, mas o gargalo permaneceu na preparação dos cashflows.

Conclusão:

Esse pipeline provou que o solver não era mais o limitante. Ele serviu como etapa intermediária para isolar o verdadeiro gargalo.

## 3. Pré-processamento de Cashflows + Cálculo por Dimensão

### 3.1 Pipeline de dimensão: `public_bonds_cashflows`

Arquivos:

```text
src/ml_ettj26/pipelines/curve_factory/public_bonds_cashflows/pipeline.py
src/ml_ettj26/pipelines/curve_factory/public_bonds_cashflows/nodes.py
```

Fluxo:

```text
carrega instrumentos distintos
  -> carrega calendário
  -> para cada ISIN:
       cria contrato uma vez
       gera schedule uma vez
       gera cashflows uma vez
       adiciona bd_index
       serializa campos do Cashflow
  -> salva mart_public_bonds_cashflow_dimension
  -> registra view DuckDB mart_public_bonds_cashflow_dimension
```

Papel da engine:

A engine não perde sentido. Ela passa a ser usada para construir a dimensão oficial de cashflows, em vez de ser chamada milhares de vezes no hot path.

### 3.2 Pipeline otimizado: `public_bonds_mart_dimension_batch`

Arquivo:

```text
src/ml_ettj26/pipelines/curve_factory/public_bonds_mart/pipeline_dimension_batch.py
```

Fluxo:

```text
SQL views
  -> carrega candidatos
  -> carrega dimensão de cashflows
  -> carrega calendário
  -> cria lookup por ISIN em arrays
  -> para cada linha:
       captura cashflows do ISIN
       filtra fluxos futuros por ref_bd_index
       monta pares (t, amount)
       cria YieldProblem.from_time_amount_pairs
  -> resolve em batch
  -> calcula duration a partir dos pares
  -> salva mart otimizado e falhas
```

Diferenças principais:

- Não cria contratos no loop histórico.
- Não cria schedules no loop histórico.
- Não cria objetos `Cashflow` no loop histórico.
- Não chama calendário para cada cashflow.
- Usa arrays compactos por ISIN.
- Agrega cashflows por data apenas para pricing.

Esse é o desenho esperado para maior ganho de performance.

## Views e Métricas de Qualidade do Dia

A view de candidatos foi enriquecida com métricas diárias:

Arquivo:

```text
sql/marts/public_bonds/02_mart_public_bonds_curve_candidates_and_exclusions.sql
```

Campos adicionados:

```text
numero_observacoes_dia
flag_volume
flag_cobertura_tenors
```

Regras:

```text
flag_volume:
  LOW    se observações < 8
  MEDIUM se 8 <= observações <= 12
  HIGH   se observações > 12

flag_cobertura_tenors:
  POOR   se spread de maturidades < 2 anos
  MEDIUM se spread entre 2 e 5 anos
  GOOD   se spread > 5 anos
```

Essas métricas são propagadas para os marts finais.

## Testes e Validações

Foram criados/adaptados testes para:

- `YieldProblem` com cache de `time_amount_pairs`.
- Detecção de single-cashflow.
- Fórmula fechada zero-coupon.
- Solver unitário usando `ZERO_COUPON`.
- Solver batch vetorizando single-cashflows.
- Newton batch em multi-cashflows.
- Fallback individual após falha no Newton batch.
- Preservação de falhas individuais.
- Agrupamento de multi-cashflows por quantidade de fluxos.
- Construção da dimensão de cashflows com chave `(isin, cashflow_number)`.
- Ranks de cashflow type.
- Adapter de arrays agregando fluxos na mesma data.
- Node otimizado com dimensão retornando `ZERO_COUPON` e `NEWTON_BATCH`.

Arquivos relevantes:

```text
tests/engine_product/pricing/test_yield_problem.py
tests/engine_product/pricing/test_yield_solver.py
tests/engine_product/pricing/test_yield_solver_batch.py
tests/engine_product/pricing/test_cashflow_arrays.py
tests/test_public_bonds_cashflow_dimension.py
tests/test_public_bonds_mart_batch_nodes.py
tests/test_public_bonds_mart_dimension_batch_nodes.py
```

Também foram feitos smoke tests diretos com `python` para validar o node baseado em dimensão, porque o ambiente local apresentou bloqueio de cache do `uv` em alguns momentos.

## Como Rodar

Pipeline original:

```powershell
uv run kedro run --pipeline public_bonds_mart
```

Pipeline com solver batch:

```powershell
uv run kedro run --pipeline public_bonds_mart_batch
```

Pipeline de dimensão de cashflows:

```powershell
uv run kedro run --pipeline public_bonds_cashflows
```

Pipeline otimizado com dimensão:

```powershell
uv run kedro run --pipeline public_bonds_mart_dimension_batch
```

Scripts de diagnóstico:

```powershell
uv run python src/scripts/profile_public_bonds_mart.py --limit 5000
uv run python src/scripts/profile_public_bonds_mart_batch.py --limit 5000
```

Com `cProfile`:

```powershell
uv run python src/scripts/profile_public_bonds_mart.py --limit 5000 --cprofile-out public_bonds_mart.prof
uv run python src/scripts/profile_public_bonds_mart_batch.py --limit 5000 --cprofile-out public_bonds_batch.prof
```

## Conclusão

A metodologia final separa a geração de estrutura financeira do cálculo histórico:

```text
Engine de produto:
  gera a dimensão oficial de cashflows por ISIN

Pipeline histórico:
  consome a dimensão em arrays
  filtra por ref_date/as_of_date
  monta pares (t, amount)
  resolve YTM em batch
```

Essa decisão preserva a coerência da engine e remove o gargalo medido. O solver foi otimizado, mas o maior ganho esperado vem da eliminação da reconstrução repetida de contratos, schedules, cashflows e year fractions no loop histórico.
