from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from statsmodels.regression.linear_model import RegressionResultsWrapper
from tqdm.auto import tqdm

from .core import LoadingSpecification


ModelPartition = Callable[[], RegressionResultsWrapper] | RegressionResultsWrapper


@dataclass(frozen=True)
class CurveCalculationConfig:
    """Grid and batching rules for persisted parametric models."""

    max_years: int = 20
    business_days_per_year: int = 252
    model_batch_size: int = 32
    show_progress: bool = True

    def __post_init__(self) -> None:
        if self.max_years <= 0:
            raise ValueError("max_years must be strictly positive")
        if self.business_days_per_year <= 0:
            raise ValueError(
                "business_days_per_year must be strictly positive"
            )
        if self.model_batch_size <= 0:
            raise ValueError("model_batch_size must be strictly positive")

    @property
    def grid_size(self) -> int:
        return self.max_years * self.business_days_per_year

    @classmethod
    def from_mapping(
        cls,
        values: Mapping[str, Any],
    ) -> CurveCalculationConfig:
        return cls(
            max_years=int(values.get("max_years", 20)),
            business_days_per_year=int(
                values.get("business_days_per_year", 252)
            ),
            model_batch_size=int(values.get("model_batch_size", 32)),
            show_progress=bool(values.get("show_progress", True)),
        )


def load_model_partition(
    partition: ModelPartition,
) -> RegressionResultsWrapper:
    """Load a lazy Kedro partition or return an already loaded model."""

    model = partition() if callable(partition) else partition
    if not hasattr(model, "curve_metadata"):
        raise ValueError("Statsmodels result is missing curve_metadata")
    if not hasattr(model, "params") or not hasattr(model, "pvalues"):
        raise ValueError("Partition is not a fitted Statsmodels result")
    return model


def _named_value(values: Any, name: str, index: int) -> float:
    try:
        value = values[name]
    except (KeyError, IndexError, TypeError):
        value = np.asarray(values, dtype=np.float64)[index]
    return float(value)


def _optional_float(value: Any) -> float:
    if value is None:
        return float("nan")
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return float("nan")
    return converted


class ParametricCurveCalculator:
    """Calculate a curve grid and dimension row from one fitted model."""

    def __init__(
        self,
        *,
        specification: LoadingSpecification,
        config: CurveCalculationConfig | None = None,
    ) -> None:
        self._specification = specification
        self._config = config or CurveCalculationConfig()
        self._tenor_bd = np.arange(
            1,
            self._config.grid_size + 1,
            dtype=np.int32,
        )
        self._tenor_years = (
            self._tenor_bd.astype(np.float64)
            / self._config.business_days_per_year
        )

    @property
    def specification(self) -> LoadingSpecification:
        return self._specification

    @property
    def config(self) -> CurveCalculationConfig:
        return self._config

    def calculate_curve(
        self,
        model: RegressionResultsWrapper,
    ) -> pd.DataFrame:
        metadata = self._validated_metadata(model)
        lambdas = np.array(
            [
                metadata["lambdas"][name]
                for name in self._specification.lambda_names
            ],
            dtype=np.float64,
        )
        betas = np.array(
            [
                _named_value(model.params, name, index)
                for index, name in enumerate(self._specification.beta_names)
            ],
            dtype=np.float64,
        )
        design = self._specification.design_matrix(
            self._tenor_years,
            lambdas,
        )
        fitted_rates = design @ betas
        if not np.isfinite(fitted_rates).all():
            raise ValueError("Calculated curve contains non-finite rates")

        return pd.DataFrame(
            {
                "ref_date": pd.Timestamp(metadata["reference_date"]),
                "tenor_bd": self._tenor_bd,
                "tenor_years": self._tenor_years,
                "fitted_rate": fitted_rates.astype(np.float64),
            }
        )

    def parameter_record(
        self,
        model: RegressionResultsWrapper,
    ) -> dict[str, Any]:
        metadata = self._validated_metadata(model)
        record: dict[str, Any] = {
            "ref_date": pd.Timestamp(metadata["reference_date"]),
            "model_name": self._specification.name,
            "schema_version": int(metadata.get("schema_version", 0)),
        }

        for index, name in enumerate(self._specification.beta_names):
            record[name] = _named_value(model.params, name, index)
            record[f"pvalue_{name}"] = _named_value(
                model.pvalues,
                name,
                index,
            )

        lambdas = metadata["lambdas"]
        for name in self._specification.lambda_names:
            record[name] = float(lambdas[name])
            record[f"log_{name}"] = float(np.log(lambdas[name]))

        for index, _ in enumerate(self._specification.lambda_names, start=1):
            record[f"peak_maturity_{index}"] = _optional_float(
                metadata.get("peak_maturities", {}).get(
                    f"peak_maturity_{index}"
                )
            )

        record.update(
            {
                "rmse": _optional_float(metadata.get("rmse")),
                "weighted_rmse": _optional_float(
                    metadata.get("weighted_rmse")
                ),
                "max_abs_error": _optional_float(
                    metadata.get("max_abs_error")
                ),
                "n_observations": int(
                    metadata.get("n_observations", 0)
                ),
                "matrix_rank": int(metadata.get("matrix_rank", 0)),
                "condition_number": _optional_float(
                    metadata.get("condition_number")
                ),
                "weighting_basis": metadata.get("weighting_basis"),
                "cov_type": getattr(
                    model,
                    "cov_type",
                    metadata.get("final_cov_type"),
                ),
                "optimizer_success": bool(
                    metadata.get("optimizer_success", False)
                ),
                "optimizer_nit": int(metadata.get("optimizer_nit", 0)),
                "optimizer_nfev": int(metadata.get("optimizer_nfev", 0)),
                "aic": _optional_float(getattr(model, "aic", None)),
                "bic": _optional_float(getattr(model, "bic", None)),
                "rsquared": _optional_float(
                    getattr(model, "rsquared", None)
                ),
                "rsquared_adj": _optional_float(
                    getattr(model, "rsquared_adj", None)
                ),
            }
        )
        return record

    def dimension_columns(self) -> list[str]:
        coefficient_columns = [
            column
            for beta_name in self._specification.beta_names
            for column in (beta_name, f"pvalue_{beta_name}")
        ]
        lambda_columns = [
            column
            for lambda_name in self._specification.lambda_names
            for column in (lambda_name, f"log_{lambda_name}")
        ]
        peak_columns = [
            f"peak_maturity_{index}"
            for index in range(
                1,
                len(self._specification.lambda_names) + 1,
            )
        ]
        return [
            "ref_date",
            "model_name",
            "schema_version",
            *coefficient_columns,
            *lambda_columns,
            *peak_columns,
            "rmse",
            "weighted_rmse",
            "max_abs_error",
            "n_observations",
            "matrix_rank",
            "condition_number",
            "weighting_basis",
            "cov_type",
            "optimizer_success",
            "optimizer_nit",
            "optimizer_nfev",
            "aic",
            "bic",
            "rsquared",
            "rsquared_adj",
        ]

    def _validated_metadata(
        self,
        model: RegressionResultsWrapper,
    ) -> Mapping[str, Any]:
        metadata = getattr(model, "curve_metadata", None)
        if not isinstance(metadata, Mapping):
            raise ValueError("Statsmodels result is missing curve_metadata")
        if metadata.get("model_name") != self._specification.name:
            raise ValueError(
                "Model metadata does not match calculator specification"
            )
        if not metadata.get("reference_date"):
            raise ValueError("Model metadata is missing reference_date")

        lambdas = metadata.get("lambdas")
        if not isinstance(lambdas, Mapping):
            raise ValueError("Model metadata is missing lambdas")
        missing = set(self._specification.lambda_names).difference(lambdas)
        if missing:
            raise ValueError(
                "Model metadata is missing lambdas: "
                + ", ".join(sorted(missing))
            )
        lambda_values = np.array(
            [lambdas[name] for name in self._specification.lambda_names],
            dtype=np.float64,
        )
        if not self._specification.validate_lambdas(lambda_values):
            raise ValueError("Model metadata contains invalid lambdas")
        return metadata


class ModelDimensionBuilder:
    """Build one compact analytical row per persisted daily model."""

    def __init__(self, calculator: ParametricCurveCalculator) -> None:
        self._calculator = calculator

    def build(
        self,
        model_partitions: Mapping[str, ModelPartition],
    ) -> pd.DataFrame:
        if not model_partitions:
            raise ValueError("No model partitions were provided")

        rows: list[dict[str, Any]] = []
        items = sorted(model_partitions.items())
        progress = tqdm(
            items,
            total=len(items),
            desc=(
                "Dimensão de parâmetros "
                f"{self._calculator.specification.name}"
            ),
            unit="modelo",
            disable=not self._calculator.config.show_progress,
            dynamic_ncols=True,
        )
        for partition_id, partition in progress:
            progress.set_postfix_str(partition_id, refresh=False)
            rows.append(
                self._calculator.parameter_record(
                    load_model_partition(partition)
                )
            )

        result = pd.DataFrame(rows)
        result = result[self._calculator.dimension_columns()]
        return result.sort_values("ref_date", kind="stable").reset_index(
            drop=True
        )


class CurveBatchPartitionBuilder:
    """
    Create lazy Parquet-ready batches without materializing all curve rows.

    Kedro's ``PartitionedDataset`` invokes each callable immediately before
    saving its partition, so memory is bounded by ``model_batch_size`` daily
    curves rather than the complete history.
    """

    def __init__(self, calculator: ParametricCurveCalculator) -> None:
        self._calculator = calculator

    def build(
        self,
        model_partitions: Mapping[str, ModelPartition],
    ) -> dict[str, Callable[[], pd.DataFrame]]:
        if not model_partitions:
            raise ValueError("No model partitions were provided")

        items = sorted(model_partitions.items())
        batches = [
            items[start : start + self._calculator.config.model_batch_size]
            for start in range(
                0,
                len(items),
                self._calculator.config.model_batch_size,
            )
        ]
        progress = tqdm(
            total=len(batches),
            desc=f"Curvas {self._calculator.specification.name}",
            unit="lote",
            disable=not self._calculator.config.show_progress,
            dynamic_ncols=True,
        )

        outputs: dict[str, Callable[[], pd.DataFrame]] = {}
        for batch_index, batch in enumerate(batches):
            partition_id = f"batch_{batch_index:05d}"
            outputs[partition_id] = self._lazy_batch(
                batch=batch,
                partition_id=partition_id,
                progress=progress,
            )
        return outputs

    def _lazy_batch(
        self,
        *,
        batch: Sequence[tuple[str, ModelPartition]],
        partition_id: str,
        progress: Any,
    ) -> Callable[[], pd.DataFrame]:
        def calculate_batch() -> pd.DataFrame:
            frames = [
                self._calculator.calculate_curve(
                    load_model_partition(partition)
                )
                for _, partition in batch
            ]
            result = pd.concat(frames, ignore_index=True)
            progress.set_postfix_str(partition_id, refresh=False)
            progress.update(1)
            if progress.n >= progress.total:
                progress.close()
            return result

        return calculate_batch
