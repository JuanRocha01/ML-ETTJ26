from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import register_duckdb_views


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=register_duckdb_views,
                inputs="params:duckdb_views.refined.sql_files",
                outputs=None,
                name="register_refined_duckdb_views",
            )
        ]
    )