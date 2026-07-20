from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .calculator_nodes import (
    build_nelson_siegel_curve_batches,
    build_nelson_siegel_parameter_dimension,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=build_nelson_siegel_parameter_dimension,
                inputs={
                    "model_partitions": "public_bonds_nelson_siegel_models",
                    "parameters": "params:parametric_curve_calculator",
                },
                outputs="public_bonds_nelson_siegel_parameter_dimension",
                name="build_nelson_siegel_parameter_dimension",
            ),
            node(
                func=build_nelson_siegel_curve_batches,
                inputs={
                    "model_partitions": "public_bonds_nelson_siegel_models",
                    "parameters": "params:parametric_curve_calculator",
                },
                outputs="public_bonds_nelson_siegel_curves",
                name="build_nelson_siegel_curve_batches",
            ),
        ]
    )
