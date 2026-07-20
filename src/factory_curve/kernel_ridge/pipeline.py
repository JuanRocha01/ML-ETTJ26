from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    build_kernel_ridge_curve_batches,
    build_kernel_ridge_model_dimension,
    fit_kernel_ridge_models,
    select_kernel_ridge_calibration_dates,
    tune_kernel_ridge_hyperparameters,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=select_kernel_ridge_calibration_dates,
                inputs={
                    "curve_inputs": (
                        "mart_public_bonds_curve_inputs_dimension_batch"
                    ),
                    "parameters": "params:kernel_ridge",
                },
                outputs="public_bonds_krr_calibration_dates",
                name="select_public_bonds_krr_calibration_dates",
            ),
            node(
                func=tune_kernel_ridge_hyperparameters,
                inputs={
                    "curve_inputs": (
                        "mart_public_bonds_curve_inputs_dimension_batch"
                    ),
                    "cashflow_dimension": (
                        "mart_public_bonds_cashflow_dimension"
                    ),
                    "calendar_df": "refined_ref_calendar_br_market",
                    "calibration_dates": (
                        "public_bonds_krr_calibration_dates"
                    ),
                    "parameters": "params:kernel_ridge",
                },
                outputs=[
                    "public_bonds_krr_hyperparameter_search",
                    "public_bonds_krr_selected_hyperparameters",
                ],
                name="tune_public_bonds_krr_hyperparameters",
            ),
            node(
                func=fit_kernel_ridge_models,
                inputs={
                    "curve_inputs": (
                        "mart_public_bonds_curve_inputs_dimension_batch"
                    ),
                    "cashflow_dimension": (
                        "mart_public_bonds_cashflow_dimension"
                    ),
                    "calendar_df": "refined_ref_calendar_br_market",
                    "selected_hyperparameters": (
                        "public_bonds_krr_selected_hyperparameters"
                    ),
                    "parameters": "params:kernel_ridge",
                },
                outputs="public_bonds_krr_models",
                name="fit_public_bonds_krr_models",
            ),
            node(
                func=build_kernel_ridge_model_dimension,
                inputs="public_bonds_krr_models",
                outputs="public_bonds_krr_model_dimension",
                name="build_public_bonds_krr_model_dimension",
            ),
            node(
                func=build_kernel_ridge_curve_batches,
                inputs={
                    "model_partitions": "public_bonds_krr_models",
                    "parameters": "params:kernel_ridge",
                },
                outputs="public_bonds_krr_curves",
                name="build_public_bonds_krr_curve_batches",
            ),
        ]
    )
