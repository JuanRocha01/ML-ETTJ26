from __future__ import annotations

from kedro.pipeline import Pipeline, node

from .nodes import buil_b3_di1_refined

def create_pipeline(**kwards) -> Pipeline:
    return Pipeline(
        [
            node(
                func=buil_b3_di1_refined,
                inputs= None,
                outputs="refined_b3_forward_di1",
                name="build_refined_b3_forward_di1"
            )
        ]
    )