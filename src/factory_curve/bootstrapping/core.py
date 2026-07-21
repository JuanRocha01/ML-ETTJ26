from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import numpy as np
import pandas as pd
from scipy.optimize import brentq, minimize_scalar
from tqdm.auto import tqdm

from engine_product.pricing.cashflow_arrays import (
    CashflowScheduleArrays,
    build_bd_index_lookup,
    build_cashflow_schedule_lookup,
)


CURVE_COLUMNS = [
    "ref_date",
    "tenor_bd",
    "tenor_years",
    "zero_rate",
    "discount_factor",
    "forward_rate",
    "is_bootstrap_pillar",
    "bootstrap_pillar_count",
    "source_instrument_count",
    "last_bootstrap_tenor_bd",
]

DIAGNOSTIC_COLUMNS = [
    "ref_date",
    "isin",
    "instrument_type",
    "maturity_tenor_bd",
    "market_price",
    "fitted_price",
    "price_error",
    "discount_factor",
    "zero_rate",
    "instruments_at_pillar",
    "status",
    "error_type",
    "error_message",
]


@dataclass(frozen=True)
class BootstrapConfig:
    """Numerical and sample rules for sequential discount-curve stripping."""

    start_date: pd.Timestamp = pd.Timestamp("2020-01-01")
    max_years: int = 20
    business_days_per_year: int = 252
    instrument_types: tuple[str, ...] = ("LTN", "NTN-F")
    min_maturity_bd: int = 1
    min_discount_factor: float = 1.0e-8
    max_discount_factor: float = 2.0
    root_tolerance: float = 1.0e-12
    batch_size: int = 64
    show_progress: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "start_date",
            pd.Timestamp(self.start_date).normalize(),
        )
        object.__setattr__(
            self,
            "instrument_types",
            tuple(str(value) for value in self.instrument_types),
        )
        if self.max_years <= 0 or self.business_days_per_year <= 0:
            raise ValueError("Curve horizon and business-day basis must be positive")
        if self.min_maturity_bd <= 0 or self.batch_size <= 0:
            raise ValueError("min_maturity_bd and batch_size must be positive")
        if not 0.0 < self.min_discount_factor < self.max_discount_factor:
            raise ValueError("Discount-factor bounds must be positive and ordered")
        if self.root_tolerance <= 0.0:
            raise ValueError("root_tolerance must be positive")

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> BootstrapConfig:
        return cls(
            start_date=pd.Timestamp(values.get("start_date", "2020-01-01")),
            max_years=int(values.get("max_years", 20)),
            business_days_per_year=int(
                values.get("business_days_per_year", 252)
            ),
            instrument_types=tuple(
                values.get("instrument_types", ("LTN", "NTN-F"))
            ),
            min_maturity_bd=int(values.get("min_maturity_bd", 1)),
            min_discount_factor=float(
                values.get("min_discount_factor", 1.0e-8)
            ),
            max_discount_factor=float(values.get("max_discount_factor", 2.0)),
            root_tolerance=float(values.get("root_tolerance", 1.0e-12)),
            batch_size=int(values.get("batch_size", 64)),
            show_progress=bool(values.get("show_progress", True)),
        )


@dataclass(frozen=True)
class BootstrapInstrument:
    isin: str
    instrument_type: str
    market_price: float
    tenor_bd: np.ndarray
    amount: np.ndarray

    @property
    def maturity_tenor_bd(self) -> int:
        return int(self.tenor_bd[-1])


@dataclass(frozen=True)
class DailyBootstrapResult:
    curve: pd.DataFrame
    diagnostics: pd.DataFrame


class PublicBondBootstrapper:
    """Bootstrap daily curves while reusing static per-ISIN cashflow arrays."""

    REQUIRED_COLUMNS = {
        "ref_date",
        "instrument_type",
        "isin",
        "market_pu",
    }

    def __init__(
        self,
        *,
        cashflow_dimension: pd.DataFrame,
        calendar_df: pd.DataFrame,
        config: BootstrapConfig | None = None,
    ) -> None:
        self._config = config or BootstrapConfig()
        self._cashflows_by_isin = build_cashflow_schedule_lookup(
            cashflow_dimension
        )
        self._bd_index_by_date = build_bd_index_lookup(calendar_df)

    @property
    def config(self) -> BootstrapConfig:
        return self._config

    def prepare_inputs(self, curve_inputs: pd.DataFrame) -> pd.DataFrame:
        missing = self.REQUIRED_COLUMNS.difference(curve_inputs.columns)
        if missing:
            raise ValueError(
                "Missing required curve-input columns: "
                + ", ".join(sorted(missing))
            )

        observations = curve_inputs.loc[
            :, ["ref_date", "instrument_type", "isin", "market_pu"]
        ].copy()
        observations["ref_date"] = pd.to_datetime(
            observations["ref_date"], errors="coerce"
        ).dt.normalize()
        observations["market_pu"] = pd.to_numeric(
            observations["market_pu"], errors="coerce"
        )
        prices = observations["market_pu"].to_numpy(dtype=np.float64)
        valid = (
            observations["ref_date"].notna()
            & observations["ref_date"].ge(self._config.start_date)
            & observations["instrument_type"].isin(
                self._config.instrument_types
            )
            & observations["isin"].notna()
            & np.isfinite(prices)
            & observations["market_pu"].gt(0.0)
        )
        return (
            observations.loc[valid]
            .sort_values(
                ["ref_date", "instrument_type", "isin"],
                kind="stable",
            )
            .reset_index(drop=True)
        )

    def bootstrap(self, daily_observations: pd.DataFrame) -> DailyBootstrapResult:
        reference_dates = pd.to_datetime(
            daily_observations["ref_date"]
        ).dt.normalize().unique()
        if len(reference_dates) != 1:
            raise ValueError("bootstrap requires observations from exactly one date")
        reference_date = pd.Timestamp(reference_dates[0])
        if daily_observations["isin"].astype(str).duplicated().any():
            raise ValueError(
                f"Duplicate ISINs found for {reference_date.date().isoformat()}"
            )

        ref_date_key = reference_date.date()
        if ref_date_key not in self._bd_index_by_date:
            raise ValueError(
                f"Calendar business-day index is missing for {ref_date_key}"
            )
        ref_bd_index = self._bd_index_by_date[ref_date_key]
        instruments = self._build_instruments(
            daily_observations,
            ref_bd_index=ref_bd_index,
        )
        if not instruments:
            raise ValueError("No eligible instruments remain for bootstrapping")

        node_tenors = [0]
        node_log_discounts = [0.0]
        diagnostic_rows: list[dict[str, Any]] = []
        maturity_groups: dict[int, list[BootstrapInstrument]] = {}
        for instrument in instruments:
            maturity_groups.setdefault(
                instrument.maturity_tenor_bd,
                [],
            ).append(instrument)

        for maturity_tenor_bd, group in sorted(maturity_groups.items()):
            log_discount = self._solve_pillar(
                instruments=group,
                known_tenors=np.asarray(node_tenors, dtype=np.float64),
                known_log_discounts=np.asarray(
                    node_log_discounts,
                    dtype=np.float64,
                ),
                maturity_tenor_bd=maturity_tenor_bd,
            )
            node_tenors.append(maturity_tenor_bd)
            node_log_discounts.append(log_discount)
            discount_factor = float(np.exp(log_discount))
            tenor_years = maturity_tenor_bd / self._config.business_days_per_year
            zero_rate = float(np.expm1(-log_discount / tenor_years))
            all_tenors = np.asarray(node_tenors, dtype=np.float64)
            all_logs = np.asarray(node_log_discounts, dtype=np.float64)
            for instrument in group:
                fitted_price = self._price_instrument(
                    instrument,
                    all_tenors,
                    all_logs,
                )
                diagnostic_rows.append(
                    {
                        "ref_date": reference_date,
                        "isin": instrument.isin,
                        "instrument_type": instrument.instrument_type,
                        "maturity_tenor_bd": maturity_tenor_bd,
                        "market_price": instrument.market_price,
                        "fitted_price": fitted_price,
                        "price_error": fitted_price - instrument.market_price,
                        "discount_factor": discount_factor,
                        "zero_rate": zero_rate,
                        "instruments_at_pillar": len(group),
                        "status": (
                            "BOOTSTRAPPED"
                            if len(group) == 1
                            else "BOOTSTRAPPED_SHARED_PILLAR"
                        ),
                        "error_type": None,
                        "error_message": None,
                    }
                )

        curve = self._build_curve(
            reference_date=reference_date,
            node_tenors=np.asarray(node_tenors, dtype=np.int64),
            node_log_discounts=np.asarray(
                node_log_discounts,
                dtype=np.float64,
            ),
            source_instrument_count=len(instruments),
        )
        diagnostics = pd.DataFrame(
            diagnostic_rows,
            columns=DIAGNOSTIC_COLUMNS,
        )
        return DailyBootstrapResult(curve=curve, diagnostics=diagnostics)

    def _build_instruments(
        self,
        observations: pd.DataFrame,
        *,
        ref_bd_index: int,
    ) -> list[BootstrapInstrument]:
        instruments: list[BootstrapInstrument] = []
        for row in observations.itertuples(index=False):
            isin = str(row.isin)
            schedule: CashflowScheduleArrays | None = (
                self._cashflows_by_isin.get(isin)
            )
            if schedule is None:
                raise ValueError(f"Cashflow dimension not found for isin={isin}")
            tenor_bd, amounts = schedule.future_arrays_as_of(ref_bd_index)
            if tenor_bd.size == 0:
                raise ValueError(
                    f"No eligible future cashflows found for isin={isin}"
                )
            if int(tenor_bd[-1]) < self._config.min_maturity_bd:
                continue
            tenor_bd = tenor_bd.astype(np.int64, copy=False)
            amounts = amounts.astype(np.float64, copy=False)
            if not np.isfinite(amounts).all() or (amounts <= 0.0).any():
                raise ValueError(
                    f"Cashflows must be finite and positive for isin={isin}"
                )
            instruments.append(
                BootstrapInstrument(
                    isin=isin,
                    instrument_type=str(row.instrument_type),
                    market_price=float(row.market_pu),
                    tenor_bd=tenor_bd,
                    amount=amounts,
                )
            )
        return instruments

    def _solve_pillar(
        self,
        *,
        instruments: list[BootstrapInstrument],
        known_tenors: np.ndarray,
        known_log_discounts: np.ndarray,
        maturity_tenor_bd: int,
    ) -> float:
        lower = float(np.log(self._config.min_discount_factor))
        upper = float(np.log(self._config.max_discount_factor))

        def residual(log_discount: float) -> float:
            tenors = np.append(known_tenors, maturity_tenor_bd)
            logs = np.append(known_log_discounts, log_discount)
            return (
                self._price_instrument(instruments[0], tenors, logs)
                - instruments[0].market_price
            )

        if len(instruments) == 1:
            lower_residual = residual(lower)
            upper_residual = residual(upper)
            if lower_residual == 0.0:
                return lower
            if upper_residual == 0.0:
                return upper
            if np.signbit(lower_residual) == np.signbit(upper_residual):
                raise ValueError(
                    "Market price cannot be matched inside discount-factor "
                    f"bounds at tenor_bd={maturity_tenor_bd}"
                )
            return float(
                brentq(
                    residual,
                    lower,
                    upper,
                    xtol=self._config.root_tolerance,
                    rtol=self._config.root_tolerance,
                )
            )

        def squared_errors(log_discount: float) -> float:
            tenors = np.append(known_tenors, maturity_tenor_bd)
            logs = np.append(known_log_discounts, log_discount)
            errors = np.asarray(
                [
                    self._price_instrument(instrument, tenors, logs)
                    - instrument.market_price
                    for instrument in instruments
                ],
                dtype=np.float64,
            )
            return float(errors @ errors)

        result = minimize_scalar(
            squared_errors,
            bounds=(lower, upper),
            method="bounded",
            options={"xatol": self._config.root_tolerance},
        )
        if not result.success or not np.isfinite(result.fun):
            raise ValueError(
                f"Shared bootstrap pillar failed at tenor_bd={maturity_tenor_bd}"
            )
        return float(result.x)

    @staticmethod
    def _price_instrument(
        instrument: BootstrapInstrument,
        node_tenors: np.ndarray,
        node_log_discounts: np.ndarray,
    ) -> float:
        log_discounts = np.interp(
            instrument.tenor_bd.astype(np.float64),
            node_tenors,
            node_log_discounts,
        )
        return float(instrument.amount @ np.exp(log_discounts))

    def _build_curve(
        self,
        *,
        reference_date: pd.Timestamp,
        node_tenors: np.ndarray,
        node_log_discounts: np.ndarray,
        source_instrument_count: int,
    ) -> pd.DataFrame:
        target_bd = np.arange(
            1,
            self._config.max_years * self._config.business_days_per_year + 1,
            dtype=np.int32,
        )
        target_years = target_bd / self._config.business_days_per_year
        segment_slopes = np.diff(node_log_discounts) / np.diff(node_tenors)
        segment_indices = np.searchsorted(
            node_tenors[1:],
            target_bd,
            side="left",
        )
        segment_indices = np.minimum(
            segment_indices,
            segment_slopes.size - 1,
        )
        left_tenors = node_tenors[segment_indices]
        left_logs = node_log_discounts[segment_indices]
        selected_slopes = segment_slopes[segment_indices]
        log_discounts = left_logs + selected_slopes * (
            target_bd - left_tenors
        )
        discount_factors = np.exp(log_discounts)
        zero_rates = np.expm1(-log_discounts / target_years)
        annual_forwards = np.expm1(
            -selected_slopes * self._config.business_days_per_year
        )
        pillar_tenors = node_tenors[1:]

        return pd.DataFrame(
            {
                "ref_date": reference_date,
                "tenor_bd": target_bd,
                "tenor_years": target_years,
                "zero_rate": zero_rates,
                "discount_factor": discount_factors,
                "forward_rate": annual_forwards,
                "is_bootstrap_pillar": np.isin(target_bd, pillar_tenors),
                "bootstrap_pillar_count": pillar_tenors.size,
                "source_instrument_count": source_instrument_count,
                "last_bootstrap_tenor_bd": int(pillar_tenors[-1]),
            },
            columns=CURVE_COLUMNS,
        )


def bootstrap_public_bond_curves(
    curve_inputs: pd.DataFrame,
    cashflow_dimension: pd.DataFrame,
    calendar_df: pd.DataFrame,
    parameters: Mapping[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Bootstrap all eligible dates, reusing the static cashflow dimension."""

    config = BootstrapConfig.from_mapping(parameters)
    bootstrapper = PublicBondBootstrapper(
        cashflow_dimension=cashflow_dimension,
        calendar_df=calendar_df,
        config=config,
    )
    observations = bootstrapper.prepare_inputs(curve_inputs)
    if observations.empty:
        return (
            pd.DataFrame(columns=CURVE_COLUMNS),
            pd.DataFrame(columns=DIAGNOSTIC_COLUMNS),
        )

    curve_batches: list[pd.DataFrame] = []
    current_batch: list[pd.DataFrame] = []
    diagnostics: list[pd.DataFrame] = []
    grouped = observations.groupby("ref_date", sort=True, observed=True)
    progress = tqdm(
        grouped,
        total=observations["ref_date"].nunique(),
        desc="Bootstrapping public-bond curves",
        unit="date",
        disable=not config.show_progress,
        dynamic_ncols=True,
    )
    for reference_date, daily_observations in progress:
        try:
            result = bootstrapper.bootstrap(daily_observations)
        except Exception as exc:
            diagnostics.append(
                pd.DataFrame(
                    [
                        {
                            "ref_date": pd.Timestamp(reference_date),
                            "status": "FAILED",
                            "error_type": type(exc).__name__,
                            "error_message": str(exc),
                        }
                    ],
                    columns=DIAGNOSTIC_COLUMNS,
                )
            )
            continue

        current_batch.append(result.curve)
        diagnostics.append(result.diagnostics)
        if len(current_batch) == config.batch_size:
            curve_batches.append(pd.concat(current_batch, ignore_index=True))
            current_batch.clear()

    if current_batch:
        curve_batches.append(pd.concat(current_batch, ignore_index=True))
    curves = (
        pd.concat(curve_batches, ignore_index=True)
        if curve_batches
        else pd.DataFrame(columns=CURVE_COLUMNS)
    )
    diagnostic_frame = (
        pd.concat(diagnostics, ignore_index=True)
        if diagnostics
        else pd.DataFrame(columns=DIAGNOSTIC_COLUMNS)
    )
    return curves, diagnostic_frame[DIAGNOSTIC_COLUMNS]
