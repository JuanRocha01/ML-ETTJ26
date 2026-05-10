from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    execute_duckdb_sql_files,
    load_public_bond_curve_candidates_from_duckdb,
    load_refined_calendar_from_duckdb,
)
from .nodes_batch import build_public_bonds_curve_inputs_batch


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=execute_duckdb_sql_files,
                inputs={
                    "duckdb_path": "params:duckdb.database_path",
                    "sql_files": "params:public_bonds_curve_mart.sql_files",
                },
                outputs=None,
                name="create_public_bonds_quality_views_batch",
            ),
            node(
                func=load_public_bond_curve_candidates_from_duckdb,
                inputs={
                    "duckdb_path": "params:duckdb.database_path",
                },
                outputs="public_bonds_curve_candidates_batch",
                name="load_public_bonds_curve_candidates_batch",
            ),
            node(
                func=load_refined_calendar_from_duckdb,
                inputs={
                    "duckdb_path": "params:duckdb.database_path",
                },
                outputs="refined_calendar_br_for_curve_mart_batch",
                name="load_refined_calendar_br_for_curve_mart_batch",
            ),
            node(
                func=build_public_bonds_curve_inputs_batch,
                inputs={
                    "curve_candidates": "public_bonds_curve_candidates_batch",
                    "calendar_df": "refined_calendar_br_for_curve_mart_batch",
                },
                outputs=[
                    "mart_public_bonds_curve_inputs_batch",
                    "mart_public_bonds_curve_calculation_failures_batch",
                ],
                name="build_public_bonds_curve_inputs_batch",
            ),
        ]
    )
