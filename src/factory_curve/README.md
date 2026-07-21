# Curvas paramétricas Nelson–Siegel e Svensson

Este pacote estima diariamente curvas paramétricas nominais a partir do mart
`mart_public_bonds_curve_inputs_dimension_batch`. Nelson–Siegel e Svensson
possuem módulos e pipelines Kedro separados, mas reutilizam a infraestrutura
numérica em `factory_curve.parametric`.

## Escopo e unidade dos dados

Somente observações com `ref_date >= 2020-01-01` e instrumentos `LTN` ou
`NTN-F` são consideradas. A data inicial e os instrumentos são configuráveis
em `conf/base/parameters.yml`.

Para manter consistência com a interpolação flat-forward já existente, o prazo \(T\) é a
`macaulay_duration`, expressa em anos BU/252. A variável dependente é
`market_ytm`, em taxa efetiva anual decimal. Taxa, duração e duração modificada
devem ser finitas e economicamente válidas. O PU não participa mais da
ponderação nem é uma coluna obrigatória deste ajuste. Linhas inválidas são
filtradas antes do agrupamento diário; uma data com observações insuficientes
falha de forma explícita, evitando modelos silenciosamente incompletos.

Essa é uma curva paramétrica de YTM por duração, não um bootstrap exato dos
fluxos de caixa. Tratar YTM de títulos com cupom como taxa zero é uma
aproximação já presente na camada de curvas atual e deve ser reavaliada quando
a etapa de geração das curvas for implementada.

## Equações

Defina:

\[
L_1(T,\lambda)=\frac{1-e^{-\lambda T}}{\lambda T},
\qquad
L_2(T,\lambda)=L_1(T,\lambda)-e^{-\lambda T}.
\]

Nelson–Siegel:

\[
y(T)=\beta_0+\beta_1L_1(T,\lambda_1)
     +\beta_2L_2(T,\lambda_1)+\varepsilon_T.
\]

Svensson:

\[
y(T)=\beta_0+\beta_1L_1(T,\lambda_1)
     +\beta_2L_2(T,\lambda_1)
     +\beta_3L_2(T,\lambda_2)+\varepsilon_T.
\]

Os loadings usam `expm1` e uma expansão local em torno de zero para evitar
cancelamento numérico em prazos curtos.

## Estimação perfilada: DE + WLS

Os lambdas são reestimados de forma independente em cada data, sem warm start.
O Differential Evolution (DE) do SciPy busca apenas os lambdas. A busca ocorre
em \(z_j=\log(\lambda_j)\), garantindo positividade e melhor cobertura de
ordens de magnitude.

Para cada candidato:

1. os loadings são montados;
2. os betas são estimados por `statsmodels.WLS`;
3. posto e número de condição da matriz ponderada são validados;
4. o erro quadrático médio ponderado é devolvido ao DE.

\[
\hat\beta(\lambda)=
\arg\min_\beta\sum_i w_i
\left[y_i-X_i(\lambda)\beta\right]^2
\]

\[
J(\lambda)=
\frac{\sum_i w_i
\left[y_i-X_i(\lambda)\hat\beta(\lambda)\right]^2}
{\sum_i w_i}.
\]

O ajuste final repete o WLS no melhor lambda e calcula a matriz de covariância
robusta HC3 para os diagnósticos e p-valores. Não há otimização por gradiente
dos betas. `polish=true` permite apenas o refinamento local do candidato global
dos lambdas feito internamente pelo SciPy.

## Ponderação por duração modificada

O peso padrão é a duração modificada ao quadrado:

\[
w_i=\left(D^{mod}_i\right)^2.
\]

Os pesos são divididos pela média da data. A normalização não altera os betas
nem o mínimo do DE; apenas mantém a escala numérica da função objetivo estável.

O quadrado é intencional. Pela aproximação de primeira ordem
\(\Delta P/P\approx-D^{mod}\Delta y\), minimizar
\((D^{mod})^2(\Delta y)^2\) equivale a minimizar o erro quadrático relativo de
preço. Diferentemente do DV01, essa regra não atribui influência adicional a um
título apenas porque seu PU ou valor monetário é maior.

O WLS não impõe normalidade aos resíduos para estimar os coeficientes. A
normalidade seria relevante para inferência exata em amostras pequenas. Por
isso, o ajuste final usa covariância robusta HC3, embora essa correção não
elimine a incerteza decorrente da estimação prévia dos lambdas.

Limitações:

- duração modificada não mede liquidez nem qualidade da cotação;
- títulos longos e sensíveis recebem maior influência por construção;
- `modified_duration_weight_power` é configurável para análises de robustez,
  mas a produção usa `2.0`;
- HC3 melhora a robustez da matriz de covariância, mas o número diário de
  títulos ainda é pequeno para inferência forte.

## Bounds e identificação

O pico do loading de curvatura ocorre aproximadamente em:

\[
T_{pico}\approx\frac{1.793282}{\lambda}.
\]

Configuração inicial:

| Modelo/fator | Lambda | Região aproximada do pico |
|---|---:|---:|
| Nelson–Siegel | 0,1793–3,5866 | 0,5–10 anos |
| Svensson, fator 1 | 0,3587–3,5866 | 0,5–5 anos |
| Svensson, fator 2 | 0,1196–0,2989 | 6–15 anos |

No Svensson, exige-se adicionalmente
\(\lambda_1/\lambda_2\geq1.2\). Isso estabiliza a identidade dos fatores,
evita troca diária de rótulos e reduz colinearidade. Os bounds já criam regiões
distintas, e a restrição permanece como defesa adicional.

Parâmetros comuns do DE:

| Parâmetro | Nelson–Siegel | Svensson |
|---|---:|---:|
| `strategy` | `best1bin` | `best1bin` |
| `init` | `sobol` | `sobol` |
| `popsize` | 16 | 16 |
| `maxiter` | 60 | 150 |
| `mutation` | 0,5–1,0 | 0,5–1,0 |
| `recombination` | 0,7 | 0,7 |
| `tol` | 1e-6 | 1e-6 |
| `polish` | verdadeiro | verdadeiro |
| `seed` | 42 | 42 |

A seed fixa torna execuções auditáveis. Ela não constitui warm start: cada
data recebe uma população Sobol gerada sem usar resultados de datas anteriores.

## Objetos salvos e metadados

Cada partição contém o próprio `RegressionResultsWrapper` do Statsmodels, sem
remover os dados do ajuste. Assim permanecem disponíveis `params`, `pvalues`,
`resid`, `fittedvalues`, `mse_resid`, `summary()` e demais diagnósticos.

O atributo `curve_metadata` acrescentado ao resultado contém:

- data, modelo, unidades e colunas de entrada;
- lambdas, log-lambdas e maturidades dos picos;
- duração modificada, fórmula e pesos efetivamente usados;
- ISINs, tenores e taxas de origem;
- RMSE simples e ponderado, maior erro, posto e condicionamento;
- status, mensagem, objetivo, iterações, avaliações e seed do DE;
- versão do schema e indicação explícita de ausência de warm start.

Os datasets Kedro são particionados por `YYYY-MM-DD.pkl`:

```text
data/06_models/factory_curve/
├── nelson_siegel/
│   └── YYYY-MM-DD.pkl
└── svensson/
    └── YYYY-MM-DD.pkl
```

Os pickles dependem das versões de Python, Statsmodels, NumPy e Pandas. O lock
do projeto deve acompanhar os artefatos para reprodutibilidade. Pickle não deve
ser carregado de origem não confiável.

Os p-valores do Statsmodels usam covariância HC3 e condicionam nos lambdas
escolhidos. Eles não incorporam a incerteza da etapa de DE e, portanto, devem
ser tratados como diagnósticos condicionais, não como inferência completa sobre
o procedimento em duas etapas.

## Pipelines

Execução separada:

```powershell
kedro run --pipeline public_bonds_nelson_siegel
kedro run --pipeline public_bonds_svensson
```

Execução conjunta:

```powershell
kedro run --pipeline public_bonds_parametric_curves
```

Cada dia é uma calibração completa. Processar todo o histórico desde 2020 pode
ser computacionalmente caro, sobretudo no Svensson, porque cada avaliação do
DE executa um WLS. Esta entrega não executa o backfill completo.

Durante a execução, cada método exibe uma barra `tqdm` com o número de datas
concluídas e a data corrente. A exibição pode ser desativada com
`show_progress: false` nos parâmetros do método.

## Calculadoras e outputs Parquet

As calculadoras consomem diretamente as partições `YYYY-MM-DD.pkl`. Para cada
modelo diário, elas recuperam os betas do objeto Statsmodels e os lambdas de
`curve_metadata`.

A dimensão de parâmetros contém uma linha por data com:

- betas e respectivos p-valores HC3;
- lambdas, log-lambdas e maturidades dos picos;
- RMSE simples e ponderado e maior erro absoluto;
- número de observações, posto, condicionamento e covariância;
- convergência do DE, AIC, BIC e \(R^2\).

Os arquivos são:

```text
data/07_model_output/factory_curve/
├── nelson_siegel/
│   ├── parameter_dimension.parquet
│   └── curves/batch_*.parquet
└── svensson/
    ├── parameter_dimension.parquet
    └── curves/batch_*.parquet
```

Cada curva contém `ref_date`, `tenor_bd`, `tenor_years` e `fitted_rate`. A
grade vai de 1 a 5.040 dias úteis, isto é, 20 anos em BU/252. `fitted_rate` é a
taxa efetiva anual produzida pela forma paramétrica; como o ajuste foi feito
sobre YTM por duração de Macaulay, ela não deve ser interpretada como uma taxa
zero livre de arbitragem sem validação adicional.

As curvas são calculadas e salvas em lotes lazy de 32 datas. Assim, o pipeline
não materializa simultaneamente todas as linhas históricas. O tamanho do lote,
horizonte e convenção são configurados em `parametric_curve_calculator`.

Execução somente das calculadoras:

```powershell
kedro run --pipeline public_bonds_nelson_siegel_curve_calculator
kedro run --pipeline public_bonds_svensson_curve_calculator
kedro run --pipeline public_bonds_parametric_curve_calculators
```

Execução completa, da estimação aos Parquets:

```powershell
kedro run --pipeline public_bonds_parametric_curves_full
```

## Estrutura e testes

```text
factory_curve/
├── parametric/       # ajuste, calculadora, dimensão e batching compartilhados
├── nelson_siegel/    # loadings, estimador e calculadora específicos
└── svensson/         # loadings, estimador e calculadora específicos
```

As abstrações de loadings e otimizador são injetáveis. Isso separa regras do
modelo, estimação e orquestração, permitindo testar cada responsabilidade sem
executar uma busca global cara.

Os testes cobrem loadings, estabilidade numérica, bounds e restrições,
ponderação por duração modificada, objetivo, equivalência WLS/OLS com pesos
iguais, recuperação dos betas, seleção temporal, metadados, serialização e
contratos Kedro. A calculadora acrescenta testes da grade, fórmulas, dimensão,
carregamento lazy e particionamento em lotes.

## Próximos passos

1. validar bounds e potência da duração modificada fora da amostra;
2. comparar resíduos em taxa e em preço relativo, estabilidade diária e
   sensibilidade a títulos individuais;
3. avaliar ajuste direto a preços/fluxos de caixa para eliminar a aproximação
   de YTM por duração;
4. definir política de retenção e compactação dos lotes Parquet;
5. avaliar uma curva zero livre de arbitragem a partir dos fluxos de caixa.
