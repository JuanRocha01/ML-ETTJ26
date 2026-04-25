from __future__ import annotations

from kedro.pipeline import Pipeline, node

from .nodes import build_swap_dipre_refined

def create_pipeline(**kwards) -> Pipeline:
    return Pipeline(
        [
            node(
                func=build_swap_dipre_refined,
                inputs= None,
                outputs="refined_b3_swap_dipre",
                name="build_refined_b3_swap_dipre"
            )
        ]
    )