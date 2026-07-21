from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import bootstrap_public_bond_curves


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=bootstrap_public_bond_curves,
                inputs={
                    "curve_inputs": (
                        "mart_public_bonds_curve_inputs_dimension_batch"
                    ),
                    "cashflow_dimension": (
                        "mart_public_bonds_cashflow_dimension"
                    ),
                    "calendar_df": "refined_ref_calendar_br_market",
                    "parameters": "params:bootstrapping",
                },
                outputs=[
                    "public_bonds_bootstrapped_curves",
                    "public_bonds_bootstrapping_diagnostics",
                ],
                name="bootstrap_public_bond_curves",
            )
        ]
    )
