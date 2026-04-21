from __future__ import annotations

from kedro.pipeline import Pipeline, node

from .nodes import build_br_calendar_refined


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=build_br_calendar_refined,
                inputs="trusted_ref_calendar_bd_index",
                outputs="refined_ref_calendar_br_market",
                name="refined_build_br_calendar",
            )
        ]
    )
