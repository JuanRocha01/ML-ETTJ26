from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import data_treatment, register_curve_duckdb_views


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=data_treatment,
                inputs={
                    "bootstrapping_curves": (
                        "public_bonds_flat_forward_curves"
                    ),
                    "nelson_siegel_curves": (
                        "public_bonds_nelson_siegel_curves"
                    ),
                    "svensson_curves": "public_bonds_svensson_curves",
                    "kernel_ridge_curves": "public_bonds_krr_curves",
                },
                outputs=[
                    "factory_curve_bootstrapping_daily",
                    "factory_curve_nelson_siegel_daily",
                    "factory_curve_svensson_daily",
                    "factory_curve_kernel_ridge_daily",
                    "factory_curve_data_treatment_complete",
                ],
                name="data_treatment",
            ),
            node(
                func=register_curve_duckdb_views,
                inputs={
                    "treatment_complete": (
                        "factory_curve_data_treatment_complete"
                    ),
                    "duckdb_path": "params:duckdb.database_path",
                    "views": "params:factory_curve_data_treatment.views",
                },
                outputs=None,
                name="register_factory_curve_duckdb_views",
            ),
        ]
    )
