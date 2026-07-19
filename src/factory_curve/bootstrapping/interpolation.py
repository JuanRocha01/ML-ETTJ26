from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol, Sequence

import numpy as np
import pandas as pd


CURVE_COLUMNS = [
    "tenor_bd",
    "tenor_years",
    "zero_rate",
    "discount_factor",
    "forward_rate",
]

BATCH_CURVE_COLUMNS = [
    "ref_date",
    *CURVE_COLUMNS,
    "source_tenor_count",
]


@dataclass(frozen=True)
class FlatForwardConfig:
    """Configuration of the target BU/252 curve grid."""

    max_years: int = 20
    business_days_per_year: int = 252

    def __post_init__(self) -> None:
        if self.max_years <= 0:
            raise ValueError("max_years must be strictly positive")
        if self.business_days_per_year <= 0:
            raise ValueError("business_days_per_year must be strictly positive")


class CurveInterpolator(Protocol):
    """Minimal dependency accepted by the batch orchestration service."""

    def interpolate(
        self,
        tenors: Sequence[float] | pd.Series,
        rates: Sequence[float] | pd.Series,
    ) -> pd.DataFrame:
        """Interpolate one curve on its configured target grid."""


class FlatForwardInterpolator:
    """
    Interpolate zero rates by keeping the forward rate flat per segment.

    Input tenors are expressed in years and rates are effective annual rates.
    Linear interpolation of log discount factors makes the continuously
    compounded forward rate constant between two consecutive knots.
    """

    def __init__(self, config: FlatForwardConfig | None = None) -> None:
        self._config = config or FlatForwardConfig()

    @property
    def config(self) -> FlatForwardConfig:
        return self._config

    def interpolate(
        self,
        tenors: Sequence[float] | pd.Series,
        rates: Sequence[float] | pd.Series,
    ) -> pd.DataFrame:
        knot_times, knot_log_discounts = self._normalize_knots(tenors, rates)
        target_bd = np.arange(
            1,
            self._config.max_years * self._config.business_days_per_year + 1,
            dtype=np.int32,
        )
        target_times = target_bd / self._config.business_days_per_year

        log_discounts, forward_rates = self._interpolate_log_discounts(
            knot_times=knot_times,
            knot_log_discounts=knot_log_discounts,
            target_times=target_times,
        )
        discount_factors = np.exp(log_discounts)
        zero_rates = np.expm1(-log_discounts / target_times)

        return pd.DataFrame(
            {
                "tenor_bd": target_bd,
                "tenor_years": target_times,
                "zero_rate": zero_rates,
                "discount_factor": discount_factors,
                "forward_rate": forward_rates,
            },
            columns=CURVE_COLUMNS,
        )

    def _normalize_knots(
        self,
        tenors: Sequence[float] | pd.Series,
        rates: Sequence[float] | pd.Series,
    ) -> tuple[np.ndarray, np.ndarray]:
        tenor_values = np.asarray(tenors, dtype=np.float64)
        rate_values = np.asarray(rates, dtype=np.float64)

        if tenor_values.ndim != 1 or rate_values.ndim != 1:
            raise ValueError("tenors and rates must be one-dimensional")
        if tenor_values.size == 0:
            raise ValueError("tenors and rates must contain at least one observation")
        if tenor_values.size != rate_values.size:
            raise ValueError("tenors and rates must have the same length")
        if not np.isfinite(tenor_values).all() or not np.isfinite(rate_values).all():
            raise ValueError("tenors and rates must contain only finite values")
        if (tenor_values <= 0.0).any():
            raise ValueError("tenors must be strictly positive")
        if (rate_values <= -1.0).any():
            raise ValueError("rates must be greater than -1")

        log_discounts = -tenor_values * np.log1p(rate_values)
        tenor_bd = np.rint(
            tenor_values * self._config.business_days_per_year
        ).astype(np.int64)
        tenor_bd = np.maximum(tenor_bd, 1)
        knots = pd.DataFrame(
            {
                "tenor_bd": tenor_bd,
                "log_discount": log_discounts,
            }
        )

        # Macaulay durations can differ by tiny floating-point amounts even
        # though they identify the same BU/252 vertex. Normalize them to the
        # output grid before consolidating LTN and NTN-F observations.
        normalized = (
            knots.groupby("tenor_bd", as_index=False, sort=True)["log_discount"]
            .mean()
            .sort_values("tenor_bd")
        )

        return (
            normalized["tenor_bd"].to_numpy(dtype=np.float64)
            / self._config.business_days_per_year,
            normalized["log_discount"].to_numpy(dtype=np.float64),
        )

    @staticmethod
    def _interpolate_log_discounts(
        *,
        knot_times: np.ndarray,
        knot_log_discounts: np.ndarray,
        target_times: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray]:
        anchored_times = np.concatenate(([0.0], knot_times))
        anchored_log_discounts = np.concatenate(([0.0], knot_log_discounts))
        segment_slopes = np.diff(anchored_log_discounts) / np.diff(anchored_times)

        # For targets after the final knot, searchsorted selects the last
        # available segment. That segment's slope is therefore extrapolated.
        segment_indices = np.searchsorted(knot_times, target_times, side="left")
        segment_indices = np.minimum(segment_indices, segment_slopes.size - 1)

        left_times = anchored_times[segment_indices]
        left_log_discounts = anchored_log_discounts[segment_indices]
        selected_slopes = segment_slopes[segment_indices]
        log_discounts = (
            left_log_discounts + selected_slopes * (target_times - left_times)
        )
        annual_effective_forwards = np.expm1(-selected_slopes)

        return log_discounts, annual_effective_forwards


class PublicBondCurveBatchBuilder:
    """
    Adapt public-bond observations to the interpolation domain.

    The interpolator is injected, keeping dataframe selection/orchestration
    separate from the financial interpolation rule.
    """

    _required_columns = {
        "ref_date",
        "instrument_type",
        "macaulay_duration",
        "market_ytm",
    }

    def __init__(
        self,
        interpolator: CurveInterpolator | None = None,
        *,
        start_date: str | pd.Timestamp = "2020-01-01",
        instrument_types: Iterable[str] = ("LTN", "NTN-F"),
        batch_size: int = 64,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be strictly positive")

        self._interpolator = interpolator or FlatForwardInterpolator()
        self._start_date = pd.Timestamp(start_date)
        self._instrument_types = frozenset(instrument_types)
        self._batch_size = batch_size

    def build(self, curve_inputs: pd.DataFrame) -> pd.DataFrame:
        missing_columns = self._required_columns.difference(curve_inputs.columns)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Missing required columns: {missing}")

        observations = self._prepare_observations(curve_inputs)
        if observations.empty:
            return self._empty_result()

        curve_batches: list[pd.DataFrame] = []
        batch: list[pd.DataFrame] = []

        for ref_date, daily_observations in observations.groupby(
            "ref_date", sort=True, observed=True
        ):
            curve = self._interpolator.interpolate(
                tenors=daily_observations["macaulay_duration"],
                rates=daily_observations["market_ytm"],
            )
            curve.insert(0, "ref_date", ref_date)
            curve["source_tenor_count"] = int(
                daily_observations["macaulay_duration"].nunique()
            )
            batch.append(curve)

            if len(batch) == self._batch_size:
                curve_batches.append(pd.concat(batch, ignore_index=True))
                batch.clear()

        if batch:
            curve_batches.append(pd.concat(batch, ignore_index=True))

        result = pd.concat(curve_batches, ignore_index=True)
        result["ref_date"] = pd.to_datetime(result["ref_date"])
        result["source_tenor_count"] = result["source_tenor_count"].astype("int16")
        return result[BATCH_CURVE_COLUMNS]

    def _prepare_observations(self, curve_inputs: pd.DataFrame) -> pd.DataFrame:
        observations = curve_inputs[
            [
                "ref_date",
                "instrument_type",
                "macaulay_duration",
                "market_ytm",
            ]
        ].copy()
        observations["ref_date"] = pd.to_datetime(
            observations["ref_date"], errors="coerce"
        )
        observations["macaulay_duration"] = pd.to_numeric(
            observations["macaulay_duration"], errors="coerce"
        )
        observations["market_ytm"] = pd.to_numeric(
            observations["market_ytm"], errors="coerce"
        )

        valid = (
            observations["ref_date"].ge(self._start_date)
            & observations["instrument_type"].isin(self._instrument_types)
            & observations["macaulay_duration"].gt(0.0)
            & observations["market_ytm"].gt(-1.0)
            & np.isfinite(observations["macaulay_duration"])
            & np.isfinite(observations["market_ytm"])
        )

        return observations.loc[valid].sort_values(
            ["ref_date", "macaulay_duration", "instrument_type"],
            kind="stable",
        )

    @staticmethod
    def _empty_result() -> pd.DataFrame:
        return pd.DataFrame(
            {
                "ref_date": pd.Series(dtype="datetime64[ns]"),
                "tenor_bd": pd.Series(dtype="int32"),
                "tenor_years": pd.Series(dtype="float64"),
                "zero_rate": pd.Series(dtype="float64"),
                "discount_factor": pd.Series(dtype="float64"),
                "forward_rate": pd.Series(dtype="float64"),
                "source_tenor_count": pd.Series(dtype="int16"),
            },
            columns=BATCH_CURVE_COLUMNS,
        )


def interpolate_flat_forward(
    tenors: Sequence[float] | pd.Series,
    rates: Sequence[float] | pd.Series,
    *,
    max_years: int = 20,
    business_days_per_year: int = 252,
) -> pd.DataFrame:
    """Functional API for one flat-forward curve."""

    interpolator = FlatForwardInterpolator(
        FlatForwardConfig(
            max_years=max_years,
            business_days_per_year=business_days_per_year,
        )
    )
    return interpolator.interpolate(tenors=tenors, rates=rates)


def interpolate_flat_forward_batch(
    curve_inputs: pd.DataFrame,
    *,
    start_date: str | pd.Timestamp = "2020-01-01",
    max_years: int = 20,
    business_days_per_year: int = 252,
    instrument_types: Iterable[str] = ("LTN", "NTN-F"),
    batch_size: int = 64,
) -> pd.DataFrame:
    """Functional API for many reference dates."""

    interpolator = FlatForwardInterpolator(
        FlatForwardConfig(
            max_years=max_years,
            business_days_per_year=business_days_per_year,
        )
    )
    builder = PublicBondCurveBatchBuilder(
        interpolator=interpolator,
        start_date=start_date,
        instrument_types=instrument_types,
        batch_size=batch_size,
    )
    return builder.build(curve_inputs)
