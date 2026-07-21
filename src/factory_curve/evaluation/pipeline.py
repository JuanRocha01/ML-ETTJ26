from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import OUTPUT_KEYS, evaluate_curve_methodologies


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=evaluate_curve_methodologies,
                inputs={
                    "flat_forward_curve": (
                        "factory_curve_flat_forward_daily"
                    ),
                    "nelson_siegel_curve": (
                        "factory_curve_nelson_siegel_daily"
                    ),
                    "svensson_curve": "factory_curve_svensson_daily",
                    "kernel_ridge_curve": (
                        "factory_curve_kernel_ridge_daily"
                    ),
                    "curve_inputs": (
                        "mart_public_bonds_curve_inputs_dimension_batch"
                    ),
                    "swap_observations": "refined_b3_swap_dipre",
                    "calendar": "refined_ref_calendar_br_market",
                    "parameters": "params:factory_curve_evaluation",
                },
                outputs=[
                    f"factory_curve_evaluation_{key}"
                    for key in OUTPUT_KEYS
                ],
                name="evaluate_curve_methodologies",
            )
        ]
    )
