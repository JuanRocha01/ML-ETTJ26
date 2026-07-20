# Kernel Ridge Regression para títulos públicos

Implementação do estimador de curva de desconto de
Filipović, Pelger e Ye, adaptada à convenção BU/252 e aos fluxos de caixa do
projeto. A fórmula e as escolhas operacionais seguem o repositório público
[`yye9701/KR_example`](https://github.com/yye9701/KR_example).

## Proteção contra data leakage

O pipeline separa explicitamente duas amostras:

1. `select_public_bonds_krr_calibration_dates` reutiliza
   `verificar_qualidade_maxima_mensal` e seleciona a primeira data de qualidade
   `HIGH / GOOD / GOOD` de cada mês;
2. somente datas estritamente anteriores a `tuning_cutoff_date` participam da
   grade LOOCV;
3. o trio escolhido (`alpha`, `delta`, `ridge`) é persistido e congelado;
4. os modelos de produção usam apenas datas a partir de
   `production_start_date`.

Com a configuração base, ambas as fronteiras são `2020-01-01`. A validação
temporal também é repetida no nó de tuning, portanto um dataset intermediário
alterado que contenha 2020 ou anos posteriores é rejeitado.

## Estimador

Para preços \(P\), matriz de fluxos \(C\), kernel \(K\) e
\(\Lambda=\operatorname{diag}(\lambda/\omega_i)\), a solução é:

\[
\hat g(t)=1+k(t,x)^\top\beta
\]

\[
\beta=C^\top(CKC^\top+\Lambda)^{-1}(P-C\mathbf{1}).
\]

Como no código de referência, o ridge é dividido pelo maior tenor do dia e:

\[
\omega_i^{-1}=n(D_i^{mod}P_i)^2.
\]

Títulos com menos de 90 dias úteis até o vencimento são removidos. A busca em
grade minimiza o RMSE LOOCV em unidades aproximadas de YTM. O cálculo usa a
identidade PRESS do linear smoother, que é equivalente ao leave-one-out
explícito sem refazer o ajuste uma vez por título.

A grade de `alpha` vai até `0.20`, em vez de terminar em `0.10`, para acomodar
o nível historicamente mais alto das taxas brasileiras e evitar que a solução
fique artificialmente presa à borda do espaço de busca.

## Artefatos

- `calibration_dates.parquet`: datas pré-2020 efetivamente usadas;
- `hyperparameter_search.parquet`: resultado completo da grade;
- `selected_hyperparameters.parquet`: combinação vencedora;
- `models/YYYY-MM-DD.pkl`: coeficientes e diagnósticos de cada data produtiva;
- `model_dimension.parquet`: uma linha de diagnóstico por modelo;
- `curves/batch_*.parquet`: fatores de desconto e taxas em 1–5.040 dias úteis.

Cada curva contém `discount_factor`, `log_yield`, `fitted_rate` e uma flag de
validade do fator de desconto. `fitted_rate` é a taxa efetiva anual equivalente
ao fator de desconto estimado.

## Execução

```powershell
kedro run --pipeline public_bonds_kernel_ridge
```

O pipeline consome o mart de curvas, a dimensão estática de fluxos de caixa e
o calendário refinado já existentes.
