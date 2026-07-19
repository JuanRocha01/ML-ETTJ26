from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import build_public_bonds_flat_forward_curves


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=build_public_bonds_flat_forward_curves,
                inputs={
                    "curve_inputs": (
                        "mart_public_bonds_curve_inputs_dimension_batch"
                    ),
                    "parameters": "params:bootstrapping",
                },
                outputs="public_bonds_flat_forward_curves",
                name="build_public_bonds_flat_forward_curves",
            )
        ]
    )

