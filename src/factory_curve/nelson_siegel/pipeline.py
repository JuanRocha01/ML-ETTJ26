from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import fit_nelson_siegel_models


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=fit_nelson_siegel_models,
                inputs={
                    "curve_inputs": (
                        "mart_public_bonds_curve_inputs_dimension_batch"
                    ),
                    "parameters": "params:nelson_siegel",
                },
                outputs="public_bonds_nelson_siegel_models",
                name="fit_public_bonds_nelson_siegel_models",
            )
        ]
    )
