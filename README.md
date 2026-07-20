# ML-ETTJ26 — Estrutura a Termo de Juros com dados do mercado brasileiro

> Projeto de pesquisa aplicada e engenharia de dados para estimação e avaliação
> da Estrutura a Termo das Taxas de Juros (ETTJ) nominal brasileira a partir de
> títulos públicos federais efetivamente negociados.

**Status:** em desenvolvimento ativo (*work in progress*).

## Visão geral

O ML-ETTJ26 investiga como metodologias com hipóteses, funções-objetivo e graus
de flexibilidade distintos se comportam na construção da curva de juros
brasileira. O estudo utiliza dados observados no mercado secundário de títulos
públicos, disponibilizados pelo Departamento de Operações do Mercado Aberto
(DEMAB/BCB), e referências de mercado da B3. Séries macroeconômicas do Sistema
Gerenciador de Séries Temporais (SGS/BCB) complementam o ambiente analítico.

A principal motivação acadêmica veio de:

- **Filipović, Pelger e Ye**, em
  [*Stripping the Discount Curve — a Robust Machine Learning Approach*](https://doi.org/10.2139/ssrn.4058150),
  que formulam a estimação não paramétrica da função desconto em um Reproducing
  Kernel Hilbert Space (RKHS);
- **Luis Giovanni Faria**, em
  [*Aprendendo a curva de juros brasileira*](https://periodicos.fgv.br/rbfin/article/download/89588/85436/203839),
  que aplica e discute essas ideias no contexto dos títulos públicos federais
  brasileiros e confronta Kernel Ridge Regression e Svensson.

O objetivo deste repositório é reproduzir e estender aspectos desses resultados
com dados reais, rastreáveis e submetidos a um framework comum de tratamento,
estimação e avaliação. A proposta, entretanto, **não é eleger um modelo
universalmente superior para estimar a ETTJ**. O experimento compara
metodologias em um recorte específico — a curva nominal construída com LTN e
NTN-F — e procura evidenciar que cada abordagem resolve um problema diferente.
Qualidade de ajuste, interpretabilidade, suavidade, estabilidade temporal,
reprecificação e consistência econômica não são objetivos equivalentes.

## Metodologias comparadas

Considere o preço \(P_i\) do título \(i\), com fluxos \(C_{ij}\) pagos nos
prazos \(T_{ij}\), e uma função desconto \(g(T)\):

$$
P_i = \sum_{j=1}^{m_i} C_{ij}g(T_{ij}) + \varepsilon_i.
$$

Esse problema inverso admite soluções diferentes conforme as restrições
impostas a \(g\), a representação da curva e a função de perda escolhida.

| Metodologia | Papel no estudo | Característica central |
|---|---|---|
| **Bootstrapping flat-forward** | baseline transparente | Converte taxas observadas em fatores de desconto aproximados e interpola linearmente \(\log g(T)\), o que implica forwards constantes por segmento. |
| **Nelson–Siegel** | benchmark paramétrico parcimonioso | Representa nível, inclinação e uma curvatura com poucos parâmetros e elevada interpretabilidade econômica. |
| **Svensson** | benchmark paramétrico flexível | Acrescenta uma segunda curvatura ao Nelson–Siegel; é a família funcional empregada pela ANBIMA em sua metodologia de ETTJ. |
| **Kernel Ridge Regression** | estimador não paramétrico inspirado em Filipović–Pelger–Ye | Aprende diretamente a função desconto no espaço funcional definido pelo kernel, conciliando aderência aos preços e regularização de suavidade. |

### Nelson–Siegel e Svensson

Definindo

$$
L_1(T,\lambda)=\frac{1-e^{-\lambda T}}{\lambda T},
\qquad
L_2(T,\lambda)=L_1(T,\lambda)-e^{-\lambda T},
$$

as taxas ajustadas são dadas por

$$
y_{NS}(T)=
\beta_0+\beta_1L_1(T,\lambda_1)+\beta_2L_2(T,\lambda_1),
$$

e

$$
y_{SV}(T)=
\beta_0+\beta_1L_1(T,\lambda_1)+\beta_2L_2(T,\lambda_1)
+\beta_3L_2(T,\lambda_2).
$$

No projeto, os parâmetros de decaimento são procurados por Differential
Evolution em escala logarítmica; condicionados aos \(\lambda\), os
coeficientes \(\beta\) são estimados por Weighted Least Squares. Os pesos
baseados no quadrado da duração modificada aproximam, em primeira ordem, uma
penalização de erro relativo de preço:

$$
\frac{\Delta P_i}{P_i}\approx-D_i^{mod}\Delta y_i
\quad\Longrightarrow\quad
w_i=(D_i^{mod})^2.
$$

Trata-se, nesta implementação, de um ajuste de YTM por duração, e não de um
bootstrap exato de cada fluxo de caixa. Essa distinção é deliberadamente
mantida nos resultados para evitar comparar objetos econômicos como se fossem
idênticos.

### Kernel Ridge Regression

Para o vetor de preços \(P\), a matriz de fluxos de caixa \(C\), a matriz de
kernel \(K\) e a regularização
\(\Lambda=\operatorname{diag}(\lambda/\omega_i)\), o estimador assume a forma

$$
\widehat{g}(t)=1+k(t,x)^\top\widehat{\beta},
$$

$$
\widehat{\beta}=
C^\top(CKC^\top+\Lambda)^{-1}(P-C\mathbf{1}).
$$

Os hiperparâmetros que controlam o kernel e a regularização são selecionados
por Leave-One-Out Cross-Validation (LOOCV) em uma janela anterior ao início da
amostra produtiva. A separação temporal é validada pelo pipeline para impedir
*data leakage*. Diferentemente dos modelos paramétricos, essa abordagem estima
a função desconto diretamente a partir dos preços e dos fluxos de caixa, sem
restringi-la a uma combinação fixa de fatores exponenciais.

Mais detalhes sobre cada implementação estão em
[`factory_curve`](src/factory_curve/README.md),
[`kernel_ridge`](src/factory_curve/kernel_ridge/README.md),
[`bootstrapping`](src/factory_curve/bootstrapping/README.md) e
[`evaluation`](src/factory_curve/evaluation/README.md).

## Da pesquisa à plataforma de dados

O projeto começou como uma tentativa de reproduzir resultados empíricos da
literatura. A obtenção de uma amostra comparável, porém, exigiu implementar
muito mais do que os estimadores. Arquivos de diferentes épocas possuem
layouts, encodings e granularidades distintos; títulos com cupom exigem
calendários, convenções e cronogramas próprios; e um teste fora da amostra
demanda referências de outro segmento de mercado.

Essa evolução transformou o repositório em um **monólito de pesquisa
estruturado**, utilizado também como laboratório para práticas de engenharia de
dados e software financeiro:

- ingestão de APIs públicas, como SGS/BCB, e de arquivos públicos do DEMAB;
- aquisição e parsing de relatórios de pregão e derivativos da B3;
- *data wrangling*, normalização de schemas e tratamento de mudanças históricas
  de layout;
- pipelines reprodutíveis e catálogos de dados com **Kedro**;
- persistência analítica em **Parquet** e consultas/visões com **DuckDB**;
- hashes de conteúdo, metadados de origem, linhagem, idempotência e validações
  de qualidade;
- processamento vetorizado e em lotes, com partições carregadas de forma
  *lazy*, para reduzir tempo de execução e pressão de memória;
- separação entre regras de domínio, infraestrutura, orquestração e cálculo
  numérico.

### Fontes

| Fonte | Informação utilizada | Aplicação |
|---|---|---|
| **BCB/SGS** | SELIC (série 432) e IPCA (série 433) | contexto macroeconômico e indexadores |
| **BCB/DEMAB** | operações definitivas com títulos públicos federais no mercado secundário | amostra principal, preços, taxas e características dos instrumentos |
| **B3 Price Report** | contratos futuros de DI (DI1) | referência adicional da estrutura de juros |
| **B3 Swap Market Rates** | cotações DI × PRE | avaliação *cross-market* fora da amostra |
| **ANBIMA** | calendário de dias úteis e documentação metodológica | convenções temporais e benchmark institucional |

### Fluxo de dados

```text
Fontes públicas
      │
      ▼
01_raw ──► 02_trusted ──► 03_refined ──► 04_feature / mart
                │               │                  │
        schema e linhagem   regras de negócio   universo de curva
                                                   │
                                                   ▼
                              engine de produtos e fluxos de caixa
                                                   │
                                                   ▼
                              modelos ──► curvas ──► avaliação
```

A orquestração é feita pelo Kedro. A camada `trusted` preserva proveniência e
contratos próximos à fonte; a `refined` harmoniza tipos, unidades e regras de
negócio; e os marts produzem uma amostra diária comum para as metodologias.

## Engine de produtos e fluxos de caixa

Uma das extensões não previstas no escopo inicial foi a construção de uma
engine própria para instrumentos de renda fixa. Ela encapsula:

- calendário brasileiro e convenção BU/252;
- regras de ajuste de datas e construção de cronogramas;
- indexadores e componentes de cupom/amortização;
- representação de LTN, NTN-F e demais títulos públicos;
- geração vetorial de matrizes de fluxos de caixa;
- cálculo de preço, YTM, duração e convexidade;
- solução de YTM em lote e cenários de fluxo.

Essa camada foi necessária para que o Kernel Ridge operasse sobre preços e
fluxos economicamente consistentes, e para que as métricas de reprecificação
fossem comparáveis entre modelos.

## Framework de avaliação

Todas as metodologias são convertidas para uma grade diária comum de 1 a 5.040
dias úteis (20 anos em BU/252). O framework utiliza contratos de interface
comuns e calculadores independentes para produzir:

1. **ajuste em taxa** — erros ponto a ponto, RMSE, MAE, viés e erro máximo;
2. **reprecificação** — erro de preço dentro da amostra de LTN e fora da
   amostra com DI × PRE;
3. **dinâmica temporal por PCA** — decomposição das variações em nível,
   inclinação e curvatura;
4. **forwards e diagnósticos de não arbitragem** — derivação via
   \(\log g(T)\) e identificação de taxas implícitas economicamente anômalas;
5. **rolldown** — comparação entre a realização em \(D+1\), a previsão direta
   da curva em \(D\) e aproximações por delta/convexidade.

O propósito é avaliar dimensões complementares. Um modelo pode minimizar erro
de reprecificação e, simultaneamente, gerar dinâmica instável; outro pode
produzir fatores interpretáveis e uma curva suave, mas não capturar
irregularidades locais. Esses resultados não constituem uma ordenação absoluta
das metodologias.

Além das métricas empíricas, a suíte automatizada cobre ingestão, parsing,
linhagem, qualidade dos marts, calendários, cronogramas, fluxos de caixa,
precificação, solvers, estabilidade numérica, contratos Kedro, prevenção de
*data leakage*, serialização, batching e fórmulas de cada modelo.

```powershell
uv run pytest
```

## Estudos e notebooks

Os notebooks registram decisões, hipóteses e análises intermediárias que
complementam o código de produção:

- [`data_quality.ipynb`](study/curve%20factory/data_quality.ipynb) — análise
  exploratória, cobertura temporal, qualidade das cotações e seleção do
  universo de títulos;
- [`trusted.ipynb`](study/trusted.ipynb) — investigação dos formatos de SGS,
  DEMAB e B3 e da engenharia necessária para ingestão, auditoria e linhagem;
- [`refined.ipynb`](study/refined.ipynb) — harmonização das fontes e aplicação
  das regras de negócio;
- [`mart_tratement.ipynb`](study/mart_tratement.ipynb) — construção e
  tratamento da base analítica de títulos públicos;
- [`test_engine.ipynb`](study/test_engine.ipynb) — validação exploratória da
  engine de produtos e fluxos de caixa.

Também estão disponíveis a
[`metodologia de processamento de YTM`](docs/public_bonds_ytm_processing_methodology.md)
e a
[`análise de desempenho do processamento`](docs/public_bonds_ytm_performance_analysis.md).

## Estrutura do repositório

```text
ML-ETTJ26/
├── conf/                    # catálogos e parâmetros dos pipelines
├── docs/                    # metodologia e documentação dos datasets
├── sql/                     # views trusted/refined e construção dos marts
├── src/
│   ├── engine_product/      # domínio de instrumentos e fluxos de caixa
│   ├── factory_curve/       # estimação, curvas e avaliação
│   └── ml_ettj26/           # ingestão, transformação e orquestração Kedro
├── study/                   # notebooks de pesquisa e validação
└── tests/                   # testes unitários, de contrato e de integração
```

## Execução

O projeto requer **Python 3.12 ou superior**. Com
[`uv`](https://docs.astral.sh/uv/), o ambiente pode ser reproduzido a partir do
lockfile:

```powershell
uv sync
uv run kedro registry list
```

Exemplos de pipelines:

```powershell
# Engenharia da amostra de títulos e fluxos de caixa
uv run kedro run --pipeline public_bonds_cashflows
uv run kedro run --pipeline public_bonds_mart_dimension_batch

# Estimação das curvas
uv run kedro run --pipeline public_bonds_bootstrapping
uv run kedro run --pipeline public_bonds_kernel_ridge
uv run kedro run --pipeline public_bonds_parametric_curves_full

# Padronização e avaliação conjunta
uv run kedro run --pipeline factory_curve_evaluation_full
```

As rotinas dependem da disponibilidade local dos arquivos públicos de origem e
dos parâmetros definidos em [`conf/base`](conf/base). Backfills completos
podem ser computacionalmente intensivos, especialmente no tuning do Kernel
Ridge e na otimização diária do Svensson.

## Limitações e estado atual

Este é um projeto em andamento. A amplitude do problema exigiu implementações
adicionais ao plano original — em especial a engine de fluxos de caixa, a
normalização histórica das fontes, os mecanismos de processamento em lote e o
framework de avaliação. Por isso, resultados e contratos ainda podem evoluir.

O recorte corrente concentra-se na curva nominal soberana e em títulos
prefixados. Diferenças de liquidez, microestrutura, tributação, horários de
marcação e basis entre mercados não desaparecem pelo uso de uma grade comum e
devem ser consideradas na interpretação dos testes fora da amostra.

## Próximos passos

- refatorar o monólito de pesquisa, preservando as fronteiras de domínio e a
  cobertura de testes já estabelecidas;
- automatizar o pipeline incremental de ingestão, validação e atualização das
  fontes;
- consolidar experimentos reproduzíveis, versionamento de dados e relatórios de
  comparação;
- ampliar os testes de robustez temporal e sensibilidade a liquidez/outliers;
- estender a engine e o framework para novos estudos com **títulos privados**,
  curvas de crédito e diferentes classes de instrumentos de renda fixa.

## Referências principais

- Filipović, D.; Pelger, M.; Ye, Y.
  [*Stripping the Discount Curve — a Robust Machine Learning Approach*](https://doi.org/10.2139/ssrn.4058150).
- Faria, L. G.
  [*Aprendendo a curva de juros brasileira*](https://periodicos.fgv.br/rbfin/article/download/89588/85436/203839).
- ANBIMA.
  [*Estrutura a Termo das Taxas de Juros Estimada e Inflação Implícita — Metodologia*](https://www.anbima.com.br/data/files/18/42/65/50/4169E510222775E5A8A80AC2/est-termo_metodologia.pdf).
- Nelson, C. R.; Siegel, A. F.
  [*Parsimonious Modeling of Yield Curves*](https://doi.org/10.1086/296409).
- Svensson, L. E. O.
  [*Estimating and Interpreting Forward Interest Rates: Sweden 1992–1994*](https://doi.org/10.3386/w4871).

---

Este repositório tem finalidade de pesquisa e desenvolvimento. Os resultados
não constituem recomendação de investimento, marcação oficial ou metodologia
de precificação para uso em produção.
