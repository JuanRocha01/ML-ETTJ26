from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from itertools import product
from typing import Any

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

from ml_ettj26.analytics.public_bonds_quality import (
    verificar_qualidade_maxima_mensal,
)

from .core import (
    CurveDataBuilder,
    DailyCurveData,
    KernelRidgeConfig,
    fit_kernel_ridge_model,
    loocv_yield_error_squares,
)
from .model import KernelRidgeDailyModel


ModelPartition = Callable[[], KernelRidgeDailyModel] | KernelRidgeDailyModel


def select_kernel_ridge_calibration_dates(
    curve_inputs: pd.DataFrame,
    parameters: dict[str, Any],
) -> pd.DataFrame:
    """
    Select the first maximum-quality date of each month before production.

    The cutoff is exclusive by construction, making the no-leakage condition
    auditable in the persisted calibration-date dataset.
    """

    config = KernelRidgeConfig.from_mapping(parameters)
    monthly = verificar_qualidade_maxima_mensal(curve_inputs)
    selected = monthly.loc[
        monthly["primeiro_dia_qualidade_maxima"].notna()
    ].copy()
    selected["ref_date"] = pd.to_datetime(
        selected["primeiro_dia_qualidade_maxima"],
        errors="raise",
    ).dt.normalize()
    selected = selected.loc[
        selected["ref_date"].lt(config.tuning_cutoff_date)
    ].copy()
    if selected.empty:
        raise ValueError(
            "No maximum-quality monthly dates exist before "
            f"{config.tuning_cutoff_date.date().isoformat()}"
        )
    if selected["ref_date"].ge(config.tuning_cutoff_date).any():
        raise AssertionError("Calibration dates violate the tuning cutoff")

    selected["month"] = selected["mes"].astype(str)
    selected["tuning_cutoff_date"] = config.tuning_cutoff_date
    return (
        selected[
            [
                "ref_date",
                "month",
                "dias_disponiveis",
                "dias_qualidade_maxima",
                "tuning_cutoff_date",
            ]
        ]
        .sort_values("ref_date", kind="stable")
        .reset_index(drop=True)
    )


def _build_daily_datasets(
    *,
    curve_inputs: pd.DataFrame,
    cashflow_dimension: pd.DataFrame,
    calendar_df: pd.DataFrame,
    config: KernelRidgeConfig,
    selected_dates: Sequence[pd.Timestamp] | None = None,
    start_date: pd.Timestamp | None = None,
) -> list[DailyCurveData]:
    builder = CurveDataBuilder(
        cashflow_dimension=cashflow_dimension,
        calendar_df=calendar_df,
        config=config,
    )
    observations = builder.prepare_inputs(curve_inputs)
    if selected_dates is not None:
        normalized_dates = pd.DatetimeIndex(selected_dates).normalize()
        observations = observations.loc[
            observations["ref_date"].isin(normalized_dates)
        ].copy()
        missing_dates = normalized_dates.difference(
            pd.DatetimeIndex(observations["ref_date"].unique())
        )
        if not missing_dates.empty:
            missing = ", ".join(
                value.date().isoformat() for value in missing_dates
            )
            raise ValueError(
                "Selected calibration dates have no eligible observations: "
                + missing
            )
    if start_date is not None:
        observations = observations.loc[
            observations["ref_date"].ge(pd.Timestamp(start_date))
        ].copy()
    if observations.empty:
        raise ValueError("No eligible observations remain for kernel ridge")

    return [
        builder.build(group)
        for _, group in observations.groupby(
            "ref_date",
            sort=True,
            observed=True,
        )
    ]


def tune_kernel_ridge_hyperparameters(
    curve_inputs: pd.DataFrame,
    cashflow_dimension: pd.DataFrame,
    calendar_df: pd.DataFrame,
    calibration_dates: pd.DataFrame,
    parameters: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run FPY grid-search LOOCV exclusively on persisted pre-2020 dates."""

    config = KernelRidgeConfig.from_mapping(parameters)
    required_date_columns = {"ref_date", "tuning_cutoff_date"}
    missing = required_date_columns.difference(calibration_dates.columns)
    if missing:
        raise ValueError(
            "Calibration dates are missing columns: "
            + ", ".join(sorted(missing))
        )
    dates = pd.to_datetime(
        calibration_dates["ref_date"],
        errors="raise",
    ).dt.normalize()
    if dates.ge(config.tuning_cutoff_date).any():
        offending = dates.loc[dates.ge(config.tuning_cutoff_date)].min()
        raise ValueError(
            "Data leakage guard rejected calibration date "
            f"{offending.date().isoformat()}"
        )
    daily_datasets = _build_daily_datasets(
        curve_inputs=curve_inputs,
        cashflow_dimension=cashflow_dimension,
        calendar_df=calendar_df,
        config=config,
        selected_dates=dates.tolist(),
    )

    candidates = list(
        product(
            config.alpha_values,
            config.delta_values,
            config.ridge_values,
        )
    )
    records: list[dict[str, Any]] = []
    progress = tqdm(
        candidates,
        total=len(candidates),
        desc="Otimizando hiperparâmetros KR por LOOCV",
        unit="combinação",
        disable=not config.show_progress,
        dynamic_ncols=True,
    )
    for alpha, delta, ridge in progress:
        squared_errors: list[np.ndarray] = []
        failed_dates = 0
        for data in daily_datasets:
            try:
                squared_errors.append(
                    loocv_yield_error_squares(
                        data,
                        alpha=alpha,
                        delta=delta,
                        ridge=ridge,
                        config=config,
                    )
                )
            except (
                FloatingPointError,
                ValueError,
                np.linalg.LinAlgError,
            ):
                failed_dates += 1
        if squared_errors and failed_dates == 0:
            all_errors = np.concatenate(squared_errors)
            rmse = float(np.sqrt(np.mean(all_errors)))
            observation_count = int(all_errors.size)
        else:
            rmse = float("nan")
            observation_count = int(
                sum(errors.size for errors in squared_errors)
            )
        records.append(
            {
                "alpha": float(alpha),
                "delta": float(delta),
                "ridge": float(ridge),
                "loocv_rmse_yield_approx": rmse,
                "n_observations": observation_count,
                "n_dates": len(daily_datasets) - failed_dates,
                "n_failed_dates": failed_dates,
                "tuning_first_date": min(
                    data.reference_date for data in daily_datasets
                ),
                "tuning_last_date": max(
                    data.reference_date for data in daily_datasets
                ),
                "tuning_cutoff_date": config.tuning_cutoff_date,
            }
        )

    search = pd.DataFrame(records)
    valid = search.loc[
        search["n_failed_dates"].eq(0)
        & search["loocv_rmse_yield_approx"].notna()
    ]
    if valid.empty:
        raise RuntimeError(
            "No hyperparameter combination completed all calibration dates"
        )
    best_index = valid.sort_values(
        [
            "loocv_rmse_yield_approx",
            "delta",
            "ridge",
            "alpha",
        ],
        kind="stable",
    ).index[0]
    search["is_best"] = False
    search.loc[best_index, "is_best"] = True
    search = search.sort_values(
        ["loocv_rmse_yield_approx", "alpha", "delta", "ridge"],
        kind="stable",
        na_position="last",
    ).reset_index(drop=True)
    selected = search.loc[search["is_best"]].reset_index(drop=True)
    return search, selected


def _selected_hyperparameters(
    selected_hyperparameters: pd.DataFrame,
) -> tuple[float, float, float]:
    if len(selected_hyperparameters) != 1:
        raise ValueError(
            "selected_hyperparameters must contain exactly one row"
        )
    row = selected_hyperparameters.iloc[0]
    return float(row["alpha"]), float(row["delta"]), float(row["ridge"])


def fit_kernel_ridge_models(
    curve_inputs: pd.DataFrame,
    cashflow_dimension: pd.DataFrame,
    calendar_df: pd.DataFrame,
    selected_hyperparameters: pd.DataFrame,
    parameters: dict[str, Any],
) -> dict[str, KernelRidgeDailyModel]:
    """Fit one KR model per date from 2020 onward with frozen parameters."""

    config = KernelRidgeConfig.from_mapping(parameters)
    alpha, delta, ridge = _selected_hyperparameters(
        selected_hyperparameters
    )
    daily_datasets = _build_daily_datasets(
        curve_inputs=curve_inputs,
        cashflow_dimension=cashflow_dimension,
        calendar_df=calendar_df,
        config=config,
        start_date=config.production_start_date,
    )
    models: dict[str, KernelRidgeDailyModel] = {}
    progress = tqdm(
        daily_datasets,
        total=len(daily_datasets),
        desc="Estimando kernel ridge",
        unit="data",
        disable=not config.show_progress,
        dynamic_ncols=True,
    )
    for data in progress:
        partition_id = data.reference_date.date().isoformat()
        progress.set_postfix_str(partition_id, refresh=False)
        models[partition_id] = fit_kernel_ridge_model(
            data,
            alpha=alpha,
            delta=delta,
            ridge=ridge,
            config=config,
        )
    return models


def _load_model(partition: ModelPartition) -> KernelRidgeDailyModel:
    model = partition() if callable(partition) else partition
    if not isinstance(model, KernelRidgeDailyModel):
        raise ValueError("Partition is not a KernelRidgeDailyModel")
    return model


def build_kernel_ridge_model_dimension(
    model_partitions: Mapping[str, ModelPartition],
) -> pd.DataFrame:
    """Build one compact diagnostics row per daily KR model."""

    if not model_partitions:
        raise ValueError("No kernel-ridge model partitions were provided")
    rows = []
    for partition_id, partition in sorted(model_partitions.items()):
        model = _load_model(partition)
        rows.append(
            {
                "ref_date": pd.Timestamp(model.reference_date),
                "model_name": "kernel_ridge",
                "alpha": model.alpha,
                "delta": model.delta,
                "ridge": model.ridge,
                "n_observations": model.n_observations,
                "n_cashflow_tenors": model.cashflow_tenors_bd.size,
                "max_cashflow_bd": model.max_cashflow_bd,
                "price_rmse": model.price_rmse,
                "weighted_yield_rmse_approx": (
                    model.weighted_yield_rmse_approx
                ),
                "max_abs_price_error": model.max_abs_price_error,
                "condition_number": model.condition_number,
                "partition_id": partition_id,
            }
        )
    return pd.DataFrame(rows).sort_values(
        "ref_date",
        kind="stable",
    ).reset_index(drop=True)


def build_kernel_ridge_curve_batches(
    model_partitions: Mapping[str, ModelPartition],
    parameters: dict[str, Any],
) -> dict[str, Callable[[], pd.DataFrame]]:
    """Return lazy Parquet-ready curve batches with bounded memory."""

    if not model_partitions:
        raise ValueError("No kernel-ridge model partitions were provided")
    config = KernelRidgeConfig.from_mapping(parameters)
    items = sorted(model_partitions.items())
    batches = [
        items[start : start + config.model_batch_size]
        for start in range(0, len(items), config.model_batch_size)
    ]
    outputs: dict[str, Callable[[], pd.DataFrame]] = {}
    for batch_index, batch in enumerate(batches):
        partition_id = f"batch_{batch_index:05d}"

        def calculate_batch(
            current_batch: list[
                tuple[str, ModelPartition]
            ] = batch,
        ) -> pd.DataFrame:
            frames = [
                _load_model(partition).curve_frame(
                    max_years=config.max_years
                )
                for _, partition in current_batch
            ]
            return pd.concat(frames, ignore_index=True)

        outputs[partition_id] = calculate_batch
    return outputs
