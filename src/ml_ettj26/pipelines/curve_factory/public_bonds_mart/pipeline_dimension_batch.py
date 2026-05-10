from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    execute_duckdb_sql_files,
    load_public_bond_curve_candidates_from_duckdb,
    load_refined_calendar_from_duckdb,
)
from .nodes_dimension_batch import (
    build_public_bonds_curve_inputs_from_cashflow_dimension,
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
                name="create_public_bonds_quality_views_dimension_batch",
            ),
            node(
                func=load_public_bond_curve_candidates_from_duckdb,
                inputs={
                    "duckdb_path": "params:duckdb.database_path",
                },
                outputs="public_bonds_curve_candidates_dimension_batch",
                name="load_public_bonds_curve_candidates_dimension_batch",
            ),
            node(
                func=load_refined_calendar_from_duckdb,
                inputs={
                    "duckdb_path": "params:duckdb.database_path",
                },
                outputs="refined_calendar_br_for_curve_mart_dimension_batch",
                name="load_refined_calendar_br_for_curve_mart_dimension_batch",
            ),
            node(
                func=build_public_bonds_curve_inputs_from_cashflow_dimension,
                inputs={
                    "curve_candidates": "public_bonds_curve_candidates_dimension_batch",
                    "cashflow_dimension": "mart_public_bonds_cashflow_dimension",
                    "calendar_df": "refined_calendar_br_for_curve_mart_dimension_batch",
                },
                outputs=[
                    "mart_public_bonds_curve_inputs_dimension_batch",
                    "mart_public_bonds_curve_calculation_failures_dimension_batch",
                ],
                name="build_public_bonds_curve_inputs_dimension_batch",
            ),
        ]
    )
