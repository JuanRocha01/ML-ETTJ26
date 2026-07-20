# Avaliação das metodologias de curva

O módulo `factory_curve.evaluation` compara `bootstrapping`,
`nelson_siegel`, `svensson` e `kernel_ridge` usando a mesma grade BU/252 e
os mesmos dados de mercado. Cada família de métricas implementa o protocolo
`MetricCalculator`; o `CurveEvaluationService` apenas injeta os dados,
executa as calculadoras e combina tabelas de mesmo contrato.

## Convenções

- taxas são efetivas anuais em decimal;
- a cotação DI x PRE refinada, armazenada em pontos percentuais, é multiplicada
  por `swap_rate_scale=0.01`;
- LTN e equivalentes zero cupom de swaps usam notional 1.000;
- o prazo é sempre medido em dias úteis e o ano possui 252 dias úteis;
- uma observação sem data ou prazo correspondente na curva não entra na
  métrica, mas permanece identificável na fonte de mercado.

## Métricas

### Ajuste em taxa

O erro é `taxa_curva - taxa_mercado`. A amostra in-sample contém somente LTN
no prazo `bd_to_maturity`. A amostra out-of-sample contém DI x PRE. São salvos
os erros ponto a ponto, métricas diárias e o resumo global com RMSE, MAE, viés
e maior erro absoluto.

### Reprecificação

Para prazo `bd`, taxa `y` e notional `N`:

```text
P = N / (1 + y) ** (bd / 252)
```

No in-sample, o preço estimado é comparado ao `market_pu` da LTN. No
out-of-sample, a taxa DI x PRE e a taxa da curva são transformadas em preços
zero cupom teóricos comparáveis. `pricing_status` distingue observações
válidas, taxas estimadas fora do domínio (`y <= -1`) e preços que excederiam a
representação numérica; somente preços válidos entram nas métricas agregadas.

### PCA temporal

O PCA é ajustado às variações diárias das curvas nos vértices mensais
(`pca_tenor_step_bd=21`). Os três componentes são associados, por máxima
similaridade dos loadings e com sinal normalizado, aos formatos de nível,
inclinação e curvatura. Scores diários e loadings são salvos separadamente.

### Forwards e não arbitragem

Primeiro calcula-se:

```text
log D(t) = -(t / 252) * log(1 + z(t))
```

O forward efetivo anual de cada intervalo da grade é derivado da diferença de
`log D`. A saída diária informa mínimo, máximo, média e proporção de violações.
As violações detalhadas incluem taxa inválida e forward fora dos limites
econômicos configurados. Por padrão, a curva nominal BRL usa o intervalo
`[0%, 100%]`; esses limites são diagnósticos configuráveis, não uma afirmação
de que taxas negativas sejam matematicamente impossíveis em todo regime.
`annualized_log_forward` preserva a magnitude de forwards tão extremos que sua
conversão para taxa efetiva não seria numericamente representável.

### Rolldown

Para cada mês, seleciona-se um dia com observações LTN repetidas em D e no
próximo dia útil: um ponto curto, dois médios e um longo, escolhidos pelos
alvos configurados. A curva de D prevê D+1 no prazo `T-1`.

São produzidas duas previsões:

1. direta, usando exatamente `z_D(T-1)`;
2. Taylor, aproximando `z_D(T-1)` com primeira e segunda derivadas centrais da
   curva e convertendo a variação de taxa em preço com delta e convexidade
   numéricos, também por diferenças centrais.

## Execução

Com as matrizes diárias já materializadas:

```powershell
kedro run --pipeline factory_curve_evaluation
```

Do tratamento das curvas até os resultados:

```powershell
kedro run --pipeline factory_curve_evaluation_full
```

Os resultados são gravados em:

```text
data/08_reporting/factory_curve/evaluation/
```
