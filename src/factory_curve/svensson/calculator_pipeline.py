from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .calculator_nodes import (
    build_svensson_curve_batches,
    build_svensson_parameter_dimension,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=build_svensson_parameter_dimension,
                inputs={
                    "model_partitions": "public_bonds_svensson_models",
                    "parameters": "params:parametric_curve_calculator",
                },
                outputs="public_bonds_svensson_parameter_dimension",
                name="build_svensson_parameter_dimension",
            ),
            node(
                func=build_svensson_curve_batches,
                inputs={
                    "model_partitions": "public_bonds_svensson_models",
                    "parameters": "params:parametric_curve_calculator",
                },
                outputs="public_bonds_svensson_curves",
                name="build_svensson_curve_batches",
            ),
        ]
    )
