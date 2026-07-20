from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from engine_product.pricing.cashflow_arrays import (
    build_bd_index_lookup,
    build_cashflow_schedule_lookup,
)

from .model import KernelRidgeDailyModel, kernel_matrix


DEFAULT_ALPHA_VALUES = tuple(np.arange(0.01, 0.201, 0.01).tolist())
DEFAULT_DELTA_VALUES = (0.0, 1.0e-6, 1.0e-5, 1.0e-4, 1.0e-3, 0.01, 0.1, 1.0)
DEFAULT_RIDGE_VALUES = (0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 50.0, 100.0)


def _finite_grid(
    values: Sequence[float],
    *,
    name: str,
    allow_zero: bool,
) -> tuple[float, ...]:
    result = tuple(float(value) for value in values)
    if not result:
        raise ValueError(f"{name} must not be empty")
    array = np.asarray(result, dtype=np.float64)
    lower_valid = array >= 0.0 if allow_zero else array > 0.0
    if not np.isfinite(array).all() or not lower_valid.all():
        qualifier = "non-negative" if allow_zero else "strictly positive"
        raise ValueError(f"{name} must contain finite, {qualifier} values")
    return result


@dataclass(frozen=True)
class KernelRidgeConfig:
    """Temporal, grid and numerical rules for the KR pipeline."""

    production_start_date: pd.Timestamp = field(
        default_factory=lambda: pd.Timestamp("2020-01-01")
    )
    tuning_cutoff_date: pd.Timestamp = field(
        default_factory=lambda: pd.Timestamp("2020-01-01")
    )
    instrument_types: tuple[str, ...] = ("LTN", "NTN-F")
    min_maturity_bd: int = 90
    min_observations: int = 4
    business_days_per_year: int = 252
    max_years: int = 20
    model_batch_size: int = 32
    show_progress: bool = True
    condition_number_limit: float = 1.0e14
    loocv_denominator_tolerance: float = 1.0e-10
    alpha_values: tuple[float, ...] = DEFAULT_ALPHA_VALUES
    delta_values: tuple[float, ...] = DEFAULT_DELTA_VALUES
    ridge_values: tuple[float, ...] = DEFAULT_RIDGE_VALUES

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "production_start_date",
            pd.Timestamp(self.production_start_date).normalize(),
        )
        object.__setattr__(
            self,
            "tuning_cutoff_date",
            pd.Timestamp(self.tuning_cutoff_date).normalize(),
        )
        object.__setattr__(
            self,
            "alpha_values",
            _finite_grid(
                self.alpha_values,
                name="alpha_values",
                allow_zero=False,
            ),
        )
        delta_values = _finite_grid(
            self.delta_values,
            name="delta_values",
            allow_zero=True,
        )
        if any(value > 1.0 for value in delta_values):
            raise ValueError("delta_values must be between zero and one")
        object.__setattr__(self, "delta_values", delta_values)
        object.__setattr__(
            self,
            "ridge_values",
            _finite_grid(
                self.ridge_values,
                name="ridge_values",
                allow_zero=False,
            ),
        )
        if self.min_maturity_bd <= 0:
            raise ValueError("min_maturity_bd must be strictly positive")
        if self.min_observations < 2:
            raise ValueError("min_observations must be at least two")
        if self.business_days_per_year <= 0:
            raise ValueError(
                "business_days_per_year must be strictly positive"
            )
        if self.max_years <= 0 or self.model_batch_size <= 0:
            raise ValueError(
                "max_years and model_batch_size must be strictly positive"
            )
        if self.condition_number_limit <= 1.0:
            raise ValueError("condition_number_limit must be greater than one")
        if self.loocv_denominator_tolerance <= 0.0:
            raise ValueError(
                "loocv_denominator_tolerance must be strictly positive"
            )

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> KernelRidgeConfig:
        grid = values.get("hyperparameter_grid", {})
        return cls(
            production_start_date=pd.Timestamp(
                values.get("production_start_date", "2020-01-01")
            ),
            tuning_cutoff_date=pd.Timestamp(
                values.get("tuning_cutoff_date", "2020-01-01")
            ),
            instrument_types=tuple(
                str(value)
                for value in values.get("instrument_types", ("LTN", "NTN-F"))
            ),
            min_maturity_bd=int(values.get("min_maturity_bd", 90)),
            min_observations=int(values.get("min_observations", 4)),
            business_days_per_year=int(
                values.get("business_days_per_year", 252)
            ),
            max_years=int(values.get("max_years", 20)),
            model_batch_size=int(values.get("model_batch_size", 32)),
            show_progress=bool(values.get("show_progress", True)),
            condition_number_limit=float(
                values.get("condition_number_limit", 1.0e14)
            ),
            loocv_denominator_tolerance=float(
                values.get("loocv_denominator_tolerance", 1.0e-10)
            ),
            alpha_values=tuple(
                grid.get("alpha", DEFAULT_ALPHA_VALUES)
            ),
            delta_values=tuple(
                grid.get("delta", DEFAULT_DELTA_VALUES)
            ),
            ridge_values=tuple(
                grid.get("ridge", DEFAULT_RIDGE_VALUES)
            ),
        )


@dataclass(frozen=True)
class DailyCurveData:
    reference_date: pd.Timestamp
    isins: tuple[str, ...]
    prices: np.ndarray
    modified_durations: np.ndarray
    cashflow_tenors_bd: np.ndarray
    cashflow_matrix: np.ndarray

    def __post_init__(self) -> None:
        prices = np.asarray(self.prices, dtype=np.float64)
        durations = np.asarray(self.modified_durations, dtype=np.float64)
        tenors = np.asarray(self.cashflow_tenors_bd, dtype=np.int64)
        cashflows = np.asarray(self.cashflow_matrix, dtype=np.float64)
        observation_count = len(self.isins)
        if prices.shape != (observation_count,):
            raise ValueError("prices must match the number of ISINs")
        if durations.shape != prices.shape:
            raise ValueError("modified_durations must match prices")
        if cashflows.shape != (observation_count, tenors.size):
            raise ValueError("cashflow_matrix has an invalid shape")
        if (
            not np.isfinite(prices).all()
            or not np.isfinite(durations).all()
            or not np.isfinite(cashflows).all()
        ):
            raise ValueError("Daily curve data must be finite")
        if (prices <= 0.0).any() or (durations <= 0.0).any():
            raise ValueError("Prices and durations must be strictly positive")
        if tenors.size == 0 or (tenors <= 0).any():
            raise ValueError("Cashflow tenors must be strictly positive")
        object.__setattr__(self, "reference_date", pd.Timestamp(self.reference_date))
        object.__setattr__(self, "prices", prices)
        object.__setattr__(self, "modified_durations", durations)
        object.__setattr__(self, "cashflow_tenors_bd", tenors)
        object.__setattr__(self, "cashflow_matrix", cashflows)

    @property
    def n_observations(self) -> int:
        return self.prices.size


class CurveDataBuilder:
    """Join daily quotes to the static cashflow dimension as compact arrays."""

    REQUIRED_INPUT_COLUMNS = {
        "ref_date",
        "instrument_type",
        "isin",
        "bd_to_maturity",
        "market_pu",
        "modified_duration",
    }

    def __init__(
        self,
        *,
        cashflow_dimension: pd.DataFrame,
        calendar_df: pd.DataFrame,
        config: KernelRidgeConfig,
    ) -> None:
        self._cashflows_by_isin = build_cashflow_schedule_lookup(
            cashflow_dimension
        )
        self._bd_index_by_date = build_bd_index_lookup(calendar_df)
        self._config = config

    def prepare_inputs(self, curve_inputs: pd.DataFrame) -> pd.DataFrame:
        missing = self.REQUIRED_INPUT_COLUMNS.difference(curve_inputs.columns)
        if missing:
            raise ValueError(
                "Missing required curve-input columns: "
                + ", ".join(sorted(missing))
            )
        observations = curve_inputs[
            sorted(self.REQUIRED_INPUT_COLUMNS)
        ].copy()
        observations["ref_date"] = pd.to_datetime(
            observations["ref_date"],
            errors="coerce",
        ).dt.normalize()
        for column in ("bd_to_maturity", "market_pu", "modified_duration"):
            observations[column] = pd.to_numeric(
                observations[column],
                errors="coerce",
            )
        numeric = observations[
            ["bd_to_maturity", "market_pu", "modified_duration"]
        ].to_numpy(dtype=np.float64)
        valid = (
            np.isfinite(numeric).all(axis=1)
            & observations["instrument_type"].isin(
                self._config.instrument_types
            )
            & observations["bd_to_maturity"].ge(
                self._config.min_maturity_bd
            )
            & observations["market_pu"].gt(0.0)
            & observations["modified_duration"].gt(0.0)
        )
        return (
            observations.loc[valid]
            .sort_values(
                ["ref_date", "bd_to_maturity", "instrument_type", "isin"],
                kind="stable",
            )
            .reset_index(drop=True)
        )

    def build(self, daily_observations: pd.DataFrame) -> DailyCurveData:
        reference_dates = pd.to_datetime(
            daily_observations["ref_date"]
        ).dt.normalize().unique()
        if len(reference_dates) != 1:
            raise ValueError("CurveDataBuilder.build requires exactly one date")
        reference_date = pd.Timestamp(reference_dates[0])
        ref_date_key = reference_date.date()
        if ref_date_key not in self._bd_index_by_date:
            raise ValueError(
                f"Calendar business-day index is missing for {ref_date_key}"
            )
        ref_bd_index = self._bd_index_by_date[ref_date_key]
        if daily_observations["isin"].astype(str).duplicated().any():
            raise ValueError(
                f"Duplicate ISINs found for {reference_date.date().isoformat()}"
            )

        schedules: list[tuple[np.ndarray, np.ndarray]] = []
        rows: list[Any] = []
        all_tenors: list[np.ndarray] = []
        for row in daily_observations.itertuples(index=False):
            isin = str(row.isin)
            schedule = self._cashflows_by_isin.get(isin)
            if schedule is None:
                raise ValueError(f"Cashflow dimension not found for isin={isin}")
            future = schedule.payment_bd_index > ref_bd_index
            tenor_bd = schedule.payment_bd_index[future] - ref_bd_index
            amounts = schedule.amount[future]
            if tenor_bd.size == 0:
                raise ValueError(
                    f"No future cashflows found for isin={isin} at "
                    f"{reference_date.date().isoformat()}"
                )
            schedules.append((tenor_bd, amounts))
            rows.append(row)
            all_tenors.append(tenor_bd)

        if len(rows) < self._config.min_observations:
            raise ValueError(
                "Kernel ridge requires at least "
                f"{self._config.min_observations} observations per date"
            )

        cashflow_tenors_bd = np.unique(np.concatenate(all_tenors))
        column_by_tenor = {
            int(tenor): index
            for index, tenor in enumerate(cashflow_tenors_bd)
        }
        cashflow_matrix = np.zeros(
            (len(rows), cashflow_tenors_bd.size),
            dtype=np.float64,
        )
        for row_index, (tenors, amounts) in enumerate(schedules):
            for tenor, amount in zip(tenors, amounts, strict=True):
                cashflow_matrix[row_index, column_by_tenor[int(tenor)]] += (
                    float(amount)
                )

        return DailyCurveData(
            reference_date=reference_date,
            isins=tuple(str(row.isin) for row in rows),
            prices=np.asarray(
                [row.market_pu for row in rows],
                dtype=np.float64,
            ),
            modified_durations=np.asarray(
                [row.modified_duration for row in rows],
                dtype=np.float64,
            ),
            cashflow_tenors_bd=cashflow_tenors_bd,
            cashflow_matrix=cashflow_matrix,
        )


def fit_kernel_ridge_model(
    data: DailyCurveData,
    *,
    alpha: float,
    delta: float,
    ridge: float,
    config: KernelRidgeConfig,
) -> KernelRidgeDailyModel:
    """Fit one closed-form KR discount curve."""

    tenor_years = (
        data.cashflow_tenors_bd.astype(np.float64)
        / config.business_days_per_year
    )
    kernel = kernel_matrix(
        tenor_years,
        tenor_years,
        alpha=alpha,
        delta=delta,
    )
    gram = data.cashflow_matrix @ kernel @ data.cashflow_matrix.T
    inverse_weights = (
        np.square(data.modified_durations * data.prices)
        * data.n_observations
    )
    ridge_scaled = float(ridge) / int(data.cashflow_tenors_bd[-1])
    system = gram + np.diag(ridge_scaled * inverse_weights)
    condition_number = float(np.linalg.cond(system))
    if (
        not np.isfinite(condition_number)
        or condition_number > config.condition_number_limit
    ):
        raise np.linalg.LinAlgError(
            "Kernel-ridge system exceeds the condition-number limit"
        )
    baseline_prices = data.cashflow_matrix.sum(axis=1)
    dual = np.linalg.solve(system, data.prices - baseline_prices)
    coefficients = data.cashflow_matrix.T @ dual
    discount_at_cashflows = 1.0 + kernel @ coefficients
    fitted_prices = data.cashflow_matrix @ discount_at_cashflows
    price_errors = data.prices - fitted_prices
    approximate_yield_errors = price_errors / (
        data.modified_durations * data.prices
    )
    return KernelRidgeDailyModel(
        reference_date=data.reference_date.date().isoformat(),
        alpha=float(alpha),
        delta=float(delta),
        ridge=float(ridge),
        business_days_per_year=config.business_days_per_year,
        cashflow_tenors_bd=data.cashflow_tenors_bd,
        coefficients=coefficients,
        n_observations=data.n_observations,
        max_cashflow_bd=int(data.cashflow_tenors_bd[-1]),
        price_rmse=float(np.sqrt(np.mean(np.square(price_errors)))),
        weighted_yield_rmse_approx=float(
            np.sqrt(np.mean(np.square(approximate_yield_errors)))
        ),
        max_abs_price_error=float(np.max(np.abs(price_errors))),
        condition_number=condition_number,
        source_isins=data.isins,
    )


def loocv_yield_error_squares(
    data: DailyCurveData,
    *,
    alpha: float,
    delta: float,
    ridge: float,
    config: KernelRidgeConfig,
) -> np.ndarray:
    """
    Calculate exact linear-smoother LOOCV errors in yield-equivalent units.

    The PRESS identity avoids refitting the model once per security. Dividing
    price errors by modified duration times price is the same first-order YTM
    error approximation induced by the FPY weighting scheme.
    """

    tenor_years = (
        data.cashflow_tenors_bd.astype(np.float64)
        / config.business_days_per_year
    )
    kernel = kernel_matrix(
        tenor_years,
        tenor_years,
        alpha=alpha,
        delta=delta,
    )
    gram = data.cashflow_matrix @ kernel @ data.cashflow_matrix.T
    inverse_weights = (
        np.square(data.modified_durations * data.prices)
        * (data.n_observations - 1)
    )
    ridge_scaled = float(ridge) / int(data.cashflow_tenors_bd[-1])
    system = gram + np.diag(ridge_scaled * inverse_weights)
    condition_number = float(np.linalg.cond(system))
    if (
        not np.isfinite(condition_number)
        or condition_number > config.condition_number_limit
    ):
        raise np.linalg.LinAlgError("Ill-conditioned LOOCV system")

    identity = np.eye(data.n_observations, dtype=np.float64)
    smoother = gram @ np.linalg.solve(system, identity)
    baseline_prices = data.cashflow_matrix.sum(axis=1)
    fitted_prices = baseline_prices + smoother @ (
        data.prices - baseline_prices
    )
    denominator = 1.0 - np.diag(smoother)
    if (
        not np.isfinite(denominator).all()
        or (
            np.abs(denominator)
            <= config.loocv_denominator_tolerance
        ).any()
    ):
        raise np.linalg.LinAlgError("Invalid LOOCV leverage denominator")
    leave_one_out_price_errors = (
        data.prices - fitted_prices
    ) / denominator
    yield_errors = leave_one_out_price_errors / (
        data.modified_durations * data.prices
    )
    squared = np.square(yield_errors)
    if not np.isfinite(squared).all():
        raise FloatingPointError("LOOCV produced non-finite errors")
    return squared
