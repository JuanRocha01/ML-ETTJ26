from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, Sequence

import numpy as np
import pandas as pd
from scipy.optimize import OptimizeResult, differential_evolution
from statsmodels.regression.linear_model import RegressionResultsWrapper, WLS
from tqdm.auto import tqdm


LARGE_PENALTY = 1.0e12
PEAK_LOCATION = 1.793282132900761


class LoadingSpecification(Protocol):
    """Model-specific loading matrix used by the shared fitting service."""

    name: str
    lambda_names: tuple[str, ...]
    beta_names: tuple[str, ...]

    def design_matrix(
        self,
        tenors: Sequence[float] | np.ndarray,
        lambdas: Sequence[float] | np.ndarray,
    ) -> np.ndarray:
        """Return the linear design matrix conditional on the lambdas."""

    def validate_lambdas(
        self,
        lambdas: Sequence[float] | np.ndarray,
    ) -> bool:
        """Return whether a candidate is admissible for this model."""


class GlobalOptimizer(Protocol):
    """Minimal optimizer dependency accepted by ``DailyCurveFitter``."""

    def optimize(
        self,
        objective: ProfiledWLSObjective,
        config: DifferentialEvolutionConfig,
    ) -> OptimizeResult:
        """Minimize the profiled objective in log-lambda space."""


@dataclass(frozen=True)
class DifferentialEvolutionConfig:
    """Configuration for the bounded global search in log-lambda space."""

    lambda_bounds: tuple[tuple[float, float], ...]
    strategy: str = "best1bin"
    popsize: int = 16
    maxiter: int = 60
    mutation: float | tuple[float, float] = (0.5, 1.0)
    recombination: float = 0.7
    tol: float = 1.0e-6
    atol: float = 0.0
    polish: bool = True
    init: str = "sobol"
    seed: int = 42
    workers: int = 1
    updating: str = "immediate"

    def __post_init__(self) -> None:
        if not self.lambda_bounds:
            raise ValueError("At least one lambda bound is required")
        for lower, upper in self.lambda_bounds:
            if not np.isfinite([lower, upper]).all():
                raise ValueError("Lambda bounds must be finite")
            if lower <= 0.0 or upper <= lower:
                raise ValueError(
                    "Each lambda bound must satisfy 0 < lower < upper"
                )
        if self.popsize <= 0:
            raise ValueError("popsize must be strictly positive")
        if self.maxiter <= 0:
            raise ValueError("maxiter must be strictly positive")
        if not 0.0 <= self.recombination <= 1.0:
            raise ValueError("recombination must be between zero and one")
        if self.workers != 1 and self.updating == "immediate":
            raise ValueError(
                "SciPy requires updating='deferred' when workers is not one"
            )

    @property
    def log_bounds(self) -> tuple[tuple[float, float], ...]:
        return tuple(
            (float(np.log(lower)), float(np.log(upper)))
            for lower, upper in self.lambda_bounds
        )

    @classmethod
    def from_mapping(
        cls,
        values: Mapping[str, Any],
        *,
        expected_lambda_count: int,
    ) -> DifferentialEvolutionConfig:
        raw_bounds = values.get("lambda_bounds")
        if raw_bounds is None:
            raise ValueError("de.lambda_bounds is required")

        bounds = tuple(
            (float(bound[0]), float(bound[1])) for bound in raw_bounds
        )
        if len(bounds) != expected_lambda_count:
            raise ValueError(
                "Expected "
                f"{expected_lambda_count} lambda bounds, received {len(bounds)}"
            )

        mutation_value = values.get("mutation", (0.5, 1.0))
        if isinstance(mutation_value, (list, tuple)):
            mutation: float | tuple[float, float] = (
                float(mutation_value[0]),
                float(mutation_value[1]),
            )
        else:
            mutation = float(mutation_value)

        return cls(
            lambda_bounds=bounds,
            strategy=str(values.get("strategy", "best1bin")),
            popsize=int(values.get("popsize", 16)),
            maxiter=int(values.get("maxiter", 60)),
            mutation=mutation,
            recombination=float(values.get("recombination", 0.7)),
            tol=float(values.get("tol", 1.0e-6)),
            atol=float(values.get("atol", 0.0)),
            polish=bool(values.get("polish", True)),
            init=str(values.get("init", "sobol")),
            seed=int(values.get("seed", 42)),
            workers=int(values.get("workers", 1)),
            updating=str(values.get("updating", "immediate")),
        )


@dataclass(frozen=True)
class CurveFitConfig:
    """Input, weighting and numerical rules shared by both curve models."""

    de: DifferentialEvolutionConfig
    start_date: pd.Timestamp = field(
        default_factory=lambda: pd.Timestamp("2020-01-01")
    )
    instrument_types: tuple[str, ...] = ("LTN", "NTN-F")
    tenor_column: str = "macaulay_duration"
    rate_column: str = "market_ytm"
    modified_duration_column: str = "modified_duration"
    min_observations: int = 4
    modified_duration_weight_power: float = 2.0
    final_cov_type: str = "HC3"
    show_progress: bool = True
    condition_number_limit: float = 1.0e10
    invalid_objective_penalty: float = LARGE_PENALTY

    def __post_init__(self) -> None:
        object.__setattr__(self, "start_date", pd.Timestamp(self.start_date))
        if self.min_observations <= 0:
            raise ValueError("min_observations must be strictly positive")
        if self.modified_duration_weight_power <= 0.0:
            raise ValueError(
                "modified_duration_weight_power must be strictly positive"
            )
        if not self.final_cov_type.strip():
            raise ValueError("final_cov_type must not be empty")
        if self.condition_number_limit <= 1.0:
            raise ValueError("condition_number_limit must be greater than one")
        if self.invalid_objective_penalty <= 0.0:
            raise ValueError("invalid_objective_penalty must be positive")

    @classmethod
    def from_mapping(
        cls,
        values: Mapping[str, Any],
        *,
        expected_lambda_count: int,
        default_min_observations: int,
    ) -> CurveFitConfig:
        return cls(
            de=DifferentialEvolutionConfig.from_mapping(
                values.get("de", {}),
                expected_lambda_count=expected_lambda_count,
            ),
            start_date=pd.Timestamp(values.get("start_date", "2020-01-01")),
            instrument_types=tuple(
                str(value)
                for value in values.get("instrument_types", ("LTN", "NTN-F"))
            ),
            tenor_column=str(values.get("tenor_column", "macaulay_duration")),
            rate_column=str(values.get("rate_column", "market_ytm")),
            modified_duration_column=str(
                values.get(
                    "modified_duration_column",
                    "modified_duration",
                )
            ),
            min_observations=int(
                values.get("min_observations", default_min_observations)
            ),
            modified_duration_weight_power=float(
                values.get("modified_duration_weight_power", 2.0)
            ),
            final_cov_type=str(values.get("final_cov_type", "HC3")),
            show_progress=bool(values.get("show_progress", True)),
            condition_number_limit=float(
                values.get("condition_number_limit", 1.0e10)
            ),
            invalid_objective_penalty=float(
                values.get("invalid_objective_penalty", LARGE_PENALTY)
            ),
        )


@dataclass(frozen=True)
class ModifiedDurationWeighting:
    """
    Convert modified duration into normalized WLS weights.

    With the default power of two, minimizing weighted squared yield errors is
    the first-order equivalent of minimizing squared relative price errors,
    because ``dP / P ~= -modified_duration * dy``. Weights are normalized to a
    daily mean of one; this leaves coefficients and the optimizer minimizer
    unchanged.
    """

    power: float = 2.0

    def __post_init__(self) -> None:
        if self.power <= 0.0:
            raise ValueError(
                "Modified-duration weight power must be strictly positive"
            )

    def calculate(
        self,
        modified_durations: Sequence[float] | np.ndarray,
    ) -> np.ndarray:
        duration_values = np.asarray(modified_durations, dtype=np.float64)

        if duration_values.ndim != 1:
            raise ValueError(
                "Modified durations must be one-dimensional"
            )
        if duration_values.size == 0:
            raise ValueError("Modified durations must not be empty")
        if (
            not np.isfinite(duration_values).all()
            or (duration_values <= 0.0).any()
        ):
            raise ValueError(
                "Modified durations must be finite and strictly positive"
            )

        raw_weights = np.power(duration_values, self.power)
        mean_weight = float(np.mean(raw_weights))
        if not np.isfinite(mean_weight) or mean_weight <= 0.0:
            raise ValueError("Modified-duration weights have an invalid mean")

        return raw_weights / mean_weight


def weighted_design_diagnostics(
    design_matrix: np.ndarray,
    weights: np.ndarray,
) -> tuple[int, float]:
    weighted_design = np.sqrt(weights)[:, None] * design_matrix
    singular_values = np.linalg.svd(weighted_design, compute_uv=False)
    rank = int(np.linalg.matrix_rank(weighted_design))
    if singular_values.size == 0 or singular_values[-1] <= 0.0:
        return rank, float("inf")
    return rank, float(singular_values[0] / singular_values[-1])


def fit_wls(
    *,
    rates: np.ndarray,
    design_matrix: np.ndarray,
    weights: np.ndarray,
    beta_names: Sequence[str],
    cov_type: str | None = None,
) -> RegressionResultsWrapper:
    exog = pd.DataFrame(design_matrix, columns=list(beta_names))
    model = WLS(endog=rates, exog=exog, weights=weights)
    if cov_type is None:
        return model.fit()
    return model.fit(cov_type=cov_type)


class ProfiledWLSObjective:
    """Profile betas by Statsmodels WLS for each DE lambda candidate."""

    def __init__(
        self,
        *,
        specification: LoadingSpecification,
        tenors: np.ndarray,
        rates: np.ndarray,
        weights: np.ndarray,
        condition_number_limit: float,
        invalid_penalty: float = LARGE_PENALTY,
    ) -> None:
        self._specification = specification
        self._tenors = np.asarray(tenors, dtype=np.float64)
        self._rates = np.asarray(rates, dtype=np.float64)
        self._weights = np.asarray(weights, dtype=np.float64)
        self._condition_number_limit = condition_number_limit
        self._invalid_penalty = invalid_penalty

    def __call__(self, log_lambdas: Sequence[float] | np.ndarray) -> float:
        try:
            log_values = np.asarray(log_lambdas, dtype=np.float64)
            if (
                log_values.shape != (len(self._specification.lambda_names),)
                or not np.isfinite(log_values).all()
            ):
                return self._invalid_penalty

            lambdas = np.exp(log_values)
            if not self._specification.validate_lambdas(lambdas):
                return self._invalid_penalty

            design = self._specification.design_matrix(self._tenors, lambdas)
            rank, condition_number = weighted_design_diagnostics(
                design,
                self._weights,
            )
            if (
                rank < design.shape[1]
                or not np.isfinite(condition_number)
                or condition_number > self._condition_number_limit
            ):
                return self._invalid_penalty

            result = fit_wls(
                rates=self._rates,
                design_matrix=design,
                weights=self._weights,
                beta_names=self._specification.beta_names,
            )
            residuals = np.asarray(result.resid, dtype=np.float64)
            objective = float(
                np.average(np.square(residuals), weights=self._weights)
            )
            if not np.isfinite(objective):
                return self._invalid_penalty
            return objective
        except (FloatingPointError, ValueError, np.linalg.LinAlgError):
            return self._invalid_penalty


class ScipyDifferentialEvolution:
    """SciPy adapter that keeps the optimizer replaceable in tests."""

    def optimize(
        self,
        objective: ProfiledWLSObjective,
        config: DifferentialEvolutionConfig,
    ) -> OptimizeResult:
        return differential_evolution(
            objective,
            bounds=config.log_bounds,
            strategy=config.strategy,
            popsize=config.popsize,
            maxiter=config.maxiter,
            mutation=config.mutation,
            recombination=config.recombination,
            tol=config.tol,
            atol=config.atol,
            polish=config.polish,
            init=config.init,
            rng=np.random.default_rng(config.seed),
            workers=config.workers,
            updating=config.updating,
        )


class DailyCurveFitter:
    """
    Fit one daily parametric curve with global lambdas and profiled WLS betas.

    Model-specific loadings and the optimizer are injected. Input selection,
    weighting, diagnostics, fitting and metadata remain shared.
    """

    def __init__(
        self,
        *,
        specification: LoadingSpecification,
        config: CurveFitConfig,
        optimizer: GlobalOptimizer | None = None,
    ) -> None:
        if len(config.de.lambda_bounds) != len(specification.lambda_names):
            raise ValueError(
                "DE bounds and model lambda dimensions must be identical"
            )
        if config.min_observations <= len(specification.beta_names):
            raise ValueError(
                "min_observations must exceed the number of beta coefficients"
            )
        self._specification = specification
        self._config = config
        self._optimizer = optimizer or ScipyDifferentialEvolution()
        self._weighting = ModifiedDurationWeighting(
            config.modified_duration_weight_power
        )

    def fit(
        self,
        daily_observations: pd.DataFrame,
    ) -> RegressionResultsWrapper:
        if len(daily_observations) < self._config.min_observations:
            raise ValueError(
                f"{self._specification.name} requires at least "
                f"{self._config.min_observations} observations per date"
            )

        tenors = daily_observations[self._config.tenor_column].to_numpy(
            dtype=np.float64
        )
        rates = daily_observations[self._config.rate_column].to_numpy(
            dtype=np.float64
        )
        modified_durations = daily_observations[
            self._config.modified_duration_column
        ].to_numpy(dtype=np.float64)
        weights = self._weighting.calculate(
            modified_durations,
        )
        objective = ProfiledWLSObjective(
            specification=self._specification,
            tenors=tenors,
            rates=rates,
            weights=weights,
            condition_number_limit=self._config.condition_number_limit,
            invalid_penalty=self._config.invalid_objective_penalty,
        )
        optimization = self._optimizer.optimize(objective, self._config.de)

        log_lambdas = np.asarray(optimization.x, dtype=np.float64)
        lambdas = np.exp(log_lambdas)
        if (
            not np.isfinite(optimization.fun)
            or optimization.fun >= self._config.invalid_objective_penalty
            or not self._specification.validate_lambdas(lambdas)
        ):
            raise RuntimeError(
                f"DE did not find an admissible {self._specification.name} fit"
            )

        design = self._specification.design_matrix(tenors, lambdas)
        rank, condition_number = weighted_design_diagnostics(design, weights)
        if rank < design.shape[1]:
            raise RuntimeError("Final WLS design matrix is rank deficient")
        if condition_number > self._config.condition_number_limit:
            raise RuntimeError(
                "Final WLS design matrix exceeds the condition-number limit"
            )

        result = fit_wls(
            rates=rates,
            design_matrix=design,
            weights=weights,
            beta_names=self._specification.beta_names,
            cov_type=self._config.final_cov_type,
        )
        residuals = np.asarray(result.resid, dtype=np.float64)
        ref_dates = pd.to_datetime(
            daily_observations["ref_date"]
        ).dt.normalize()
        unique_dates = ref_dates.unique()
        if len(unique_dates) != 1:
            raise ValueError("Daily fitter received more than one reference date")
        reference_date = pd.Timestamp(unique_dates[0])

        lambda_metadata = {
            name: float(value)
            for name, value in zip(
                self._specification.lambda_names,
                lambdas,
                strict=True,
            )
        }
        log_lambda_metadata = {
            f"log_{name}": float(value)
            for name, value in zip(
                self._specification.lambda_names,
                log_lambdas,
                strict=True,
            )
        }
        peak_metadata = {
            f"peak_maturity_{index + 1}": float(PEAK_LOCATION / value)
            for index, value in enumerate(lambdas)
        }

        result.curve_metadata = {
            "schema_version": 2,
            "model_name": self._specification.name,
            "reference_date": reference_date.date().isoformat(),
            "rate_unit": "annual_effective_decimal",
            "tenor_unit": "BU/252 years (Macaulay duration by default)",
            "tenor_column": self._config.tenor_column,
            "rate_column": self._config.rate_column,
            "instrument_types": list(self._config.instrument_types),
            "beta_names": list(self._specification.beta_names),
            "lambdas": lambda_metadata,
            "log_lambdas": log_lambda_metadata,
            "peak_maturities": peak_metadata,
            "weighting_basis": "modified_duration",
            "wls_weight_definition": (
                "modified_duration ** "
                f"{self._config.modified_duration_weight_power:g}, "
                "normalized to daily mean 1"
            ),
            "final_cov_type": self._config.final_cov_type,
            "source_modified_durations": modified_durations.tolist(),
            "weights": weights.tolist(),
            "n_observations": int(len(daily_observations)),
            "source_isins": daily_observations["isin"].astype(str).tolist(),
            "source_tenors": tenors.tolist(),
            "source_rates": rates.tolist(),
            "weighted_mse": float(
                np.average(np.square(residuals), weights=weights)
            ),
            "weighted_rmse": float(
                np.sqrt(np.average(np.square(residuals), weights=weights))
            ),
            "rmse": float(np.sqrt(np.mean(np.square(residuals)))),
            "max_abs_error": float(np.max(np.abs(residuals))),
            "matrix_rank": rank,
            "condition_number": condition_number,
            "optimizer": "scipy.optimize.differential_evolution",
            "optimizer_success": bool(optimization.success),
            "optimizer_message": str(optimization.message),
            "optimizer_fun": float(optimization.fun),
            "optimizer_nit": int(getattr(optimization, "nit", 0)),
            "optimizer_nfev": int(getattr(optimization, "nfev", 0)),
            "optimizer_seed": self._config.de.seed,
            "warm_start": False,
        }
        return result


def required_input_columns(config: CurveFitConfig) -> set[str]:
    return {
        "ref_date",
        "instrument_type",
        "isin",
        config.tenor_column,
        config.rate_column,
        config.modified_duration_column,
    }


def prepare_curve_inputs(
    curve_inputs: pd.DataFrame,
    config: CurveFitConfig,
) -> pd.DataFrame:
    """Validate, normalize and select observations from the configured start."""

    missing_columns = required_input_columns(config).difference(
        curve_inputs.columns
    )
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Missing required columns: {missing}")

    selected_columns = list(required_input_columns(config))
    observations = curve_inputs[selected_columns].copy()
    observations["ref_date"] = pd.to_datetime(
        observations["ref_date"],
        errors="coerce",
    ).dt.normalize()

    numeric_columns = [
        config.tenor_column,
        config.rate_column,
        config.modified_duration_column,
    ]
    for column in numeric_columns:
        observations[column] = pd.to_numeric(
            observations[column],
            errors="coerce",
        )

    finite_numeric = np.isfinite(
        observations[numeric_columns].to_numpy(dtype=np.float64)
    ).all(axis=1)
    valid = (
        observations["ref_date"].ge(config.start_date)
        & observations["instrument_type"].isin(config.instrument_types)
        & observations[config.tenor_column].gt(0.0)
        & observations[config.rate_column].gt(-1.0)
        & observations[config.modified_duration_column].gt(0.0)
        & finite_numeric
    )

    return (
        observations.loc[valid]
        .sort_values(
            ["ref_date", config.tenor_column, "instrument_type", "isin"],
            kind="stable",
        )
        .reset_index(drop=True)
    )


def fit_models_by_date(
    curve_inputs: pd.DataFrame,
    *,
    specification: LoadingSpecification,
    config: CurveFitConfig,
    optimizer: GlobalOptimizer | None = None,
) -> dict[str, RegressionResultsWrapper]:
    """Fit and return one serializable Statsmodels result per reference date."""

    observations = prepare_curve_inputs(curve_inputs, config)
    if observations.empty:
        raise ValueError(
            "No eligible curve observations remain after input selection"
        )
    fitter = DailyCurveFitter(
        specification=specification,
        config=config,
        optimizer=optimizer,
    )
    models: dict[str, RegressionResultsWrapper] = {}

    daily_groups = observations.groupby(
        "ref_date",
        sort=True,
        observed=True,
    )
    progress = tqdm(
        daily_groups,
        total=observations["ref_date"].nunique(),
        desc=f"Estimando {specification.name}",
        unit="data",
        disable=not config.show_progress,
        dynamic_ncols=True,
    )

    for reference_date, daily_observations in progress:
        partition_id = pd.Timestamp(reference_date).date().isoformat()
        progress.set_postfix_str(partition_id, refresh=False)
        models[partition_id] = fitter.fit(daily_observations)

    return models
