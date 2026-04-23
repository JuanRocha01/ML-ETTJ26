from __future__ import annotations

from kedro.pipeline import Pipeline, node

from .nodes import build_demab_refined


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=build_demab_refined,
                inputs=None,
                outputs="refined_bcb_demab_secondary_market",
                name="build_refined_bcb_demab",
            )
        ]
    )
