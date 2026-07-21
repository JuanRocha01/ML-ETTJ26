# Public-bond flat-forward interpolation

This package builds one nominal term structure per `ref_date` from LTN and
NTN-F observations. It interpolates approximate discount factors inferred
from observed YTMs; it does not bootstrap cash flows or strip an exact zero
curve.

## Method

`macaulay_duration` is the curve tenor, expressed in BU/252 years. Observed
annual effective yields are converted to log discount factors:

```text
log(DF(t)) = -t * log(1 + y(t))
```

Durations are assigned to the nearest BU/252 vertex. If more than one bond
occupies the same vertex, their log discount factors are averaged. Linear
interpolation of `log(DF)` makes the forward rate constant inside each
segment. The first segment is anchored at `(0, DF=1)` and the last segment is
extended through the 20-year horizon.

The output rates (`zero_rate` and `forward_rate`) are annual effective decimal
rates. `tenor_bd` ranges from 1 to 5,040.

## Kedro pipeline

Run:

```powershell
kedro run --pipeline public_bonds_flat_forward
```

Input:

```text
mart_public_bonds_curve_inputs_dimension_batch
```

Output:

```text
data/curves/public_bonds_flat_forward_curves.parquet
```

The start date, horizon, BU convention, instruments, and calculation batch
size are configured under `flat_forward` in `conf/base/parameters.yml`.
