from __future__ import annotations

from kedro.pipeline import Pipeline, node

from .nodes import build_anbima_holidays_trusted, build_calendar_bd_index_trusted


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=build_anbima_holidays_trusted,
                inputs=["raw_anbima_holidays", "params:anbima_calendar"],
                outputs="trusted_ref_anbima_holidays",
                name="trusted_build_anbima_holidays",
            ),
            node(
                func=build_calendar_bd_index_trusted,
                inputs=["trusted_ref_anbima_holidays", "params:anbima_calendar"],
                outputs="trusted_ref_calendar_bd_index",
                name="trusted_build_calendar_bd_index",
            ),
        ]
    )
