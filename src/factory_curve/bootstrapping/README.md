# Public-bond cashflow bootstrapping

This package strips daily discount curves from LTN and NTN-F market prices.
Unlike `factory_curve.flat_forward`, it prices every future cashflow from the
static `mart_public_bonds_cashflow_dimension` and solves discount factors
sequentially by final payment tenor.

Between solved pillars, log discount factors are linear. Coupon dates between
the previous pillar and the pillar currently being solved therefore depend on
the candidate discount factor and are included in the scalar root problem.
This makes each unique-maturity instrument reprice exactly, subject to the
configured numerical tolerance. Instruments sharing a maturity are fitted to
one common pillar by scalar least squares and are identified in diagnostics.

The cashflow dimension is built once from the product engine. Daily processing
only slices compact NumPy arrays by the reference-date business-day index; it
does not recreate contracts, schedules, calendars, or Cashflow objects.

Only observations on or after `2020-01-01` are processed by default.

Run:

```powershell
kedro run --pipeline public_bonds_bootstrapping
```
