from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import build_b3_swap_trusted_range_partitioned


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=build_b3_swap_trusted_range_partitioned,
                inputs=dict(
                    raw_dir="params:b3_swap.raw_dir",
                    start_date="params:b3_swap.start_date",
                    end_date="params:b3_swap.end_date",
                    product_specs_by_code="params:b3_swap.product_specs_by_code",
                    product_specs_by_name="params:b3_swap.product_specs_by_name",
                    encoding="params:b3_swap.encoding",
                    target_cod_prod="params:b3_swap.target_cod_prod",
                ),
                outputs=[
                    "trusted_b3_swap_dixpre_partitioned",
                    "trusted_b3_swap_master",
                    "trusted_b3_swap_data_lineage",
                ],
                name="build_b3_swap_trusted_range_partitioned_node",
            ),
        ]
    )
