from __future__ import annotations

from kedro.pipeline import Pipeline, node

from .nodes import (
    load_trusted_calendar_from_duckdb,
    build_br_calendar_refined,
)


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=load_trusted_calendar_from_duckdb,
                inputs=None,
                outputs="trusted_ref_calendar_bd_index_from_duckdb",
                name="load_trusted_calendar_from_duckdb",
            ),
            node(
                func=build_br_calendar_refined,
                inputs="trusted_ref_calendar_bd_index_from_duckdb",
                outputs="refined_ref_calendar_br_market",
                name="refined_build_br_calendar",
            ),
        ]
    )
