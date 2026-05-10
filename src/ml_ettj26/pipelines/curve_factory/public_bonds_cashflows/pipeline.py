from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    build_public_bond_cashflow_dimension,
    execute_duckdb_sql_files,
    load_public_bond_instruments_from_duckdb,
    load_refined_calendar_from_duckdb,
    register_public_bond_cashflow_dimension_view,
)


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=execute_duckdb_sql_files,
                inputs={
                    "duckdb_path": "params:duckdb.database_path",
                    "sql_files": "params:public_bonds_cashflow_dimension.sql_files",
                },
                outputs=None,
                name="create_public_bond_cashflow_source_views",
            ),
            node(
                func=load_public_bond_instruments_from_duckdb,
                inputs={
                    "duckdb_path": "params:duckdb.database_path",
                },
                outputs="public_bond_cashflow_dimension_instruments",
                name="load_public_bond_cashflow_dimension_instruments",
            ),
            node(
                func=load_refined_calendar_from_duckdb,
                inputs={
                    "duckdb_path": "params:duckdb.database_path",
                },
                outputs="refined_calendar_br_for_cashflow_dimension",
                name="load_refined_calendar_br_for_cashflow_dimension",
            ),
            node(
                func=build_public_bond_cashflow_dimension,
                inputs={
                    "instruments": "public_bond_cashflow_dimension_instruments",
                    "calendar_df": "refined_calendar_br_for_cashflow_dimension",
                },
                outputs="mart_public_bonds_cashflow_dimension",
                name="build_public_bonds_cashflow_dimension",
            ),
            node(
                func=register_public_bond_cashflow_dimension_view,
                inputs={
                    "duckdb_path": "params:duckdb.database_path",
                    "parquet_path": "params:public_bonds_cashflow_dimension.parquet_path",
                    "cashflow_dimension": "mart_public_bonds_cashflow_dimension",
                },
                outputs=None,
                name="register_public_bonds_cashflow_dimension_view",
            ),
        ]
    )
