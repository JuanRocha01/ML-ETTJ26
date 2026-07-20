from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import fit_svensson_models


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=fit_svensson_models,
                inputs={
                    "curve_inputs": (
                        "mart_public_bonds_curve_inputs_dimension_batch"
                    ),
                    "parameters": "params:svensson",
                },
                outputs="public_bonds_svensson_models",
                name="fit_public_bonds_svensson_models",
            )
        ]
    )
