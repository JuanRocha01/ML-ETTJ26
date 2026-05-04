from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    build_public_bonds_curve_inputs,
    execute_duckdb_sql_files,
    load_public_bond_curve_candidates_from_duckdb,
    load_refined_calendar_from_duckdb,
)


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
                name="create_public_bonds_quality_views",
            ),
            node(
                func=load_public_bond_curve_candidates_from_duckdb,
                inputs={
                    "duckdb_path": "params:duckdb.database_path",
                },
                outputs="public_bonds_curve_candidates",
                name="load_public_bonds_curve_candidates",
            ),
            node(
                func=load_refined_calendar_from_duckdb,
                inputs={
                    "duckdb_path": "params:duckdb.database_path",
                },
                outputs="refined_calendar_br_for_curve_mart",
                name="load_refined_calendar_br_for_curve_mart",
            ),
            node(
                func=build_public_bonds_curve_inputs,
                inputs={
                    "curve_candidates": "public_bonds_curve_candidates",
                    "calendar_df": "refined_calendar_br_for_curve_mart",
                },
                outputs=[
                    "mart_public_bonds_curve_inputs",
                    "mart_public_bonds_curve_calculation_failures",
                ],
                name="build_public_bonds_curve_inputs",
            ),
        ]
    )