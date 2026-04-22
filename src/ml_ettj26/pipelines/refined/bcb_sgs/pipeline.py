from __future__ import annotations

from kedro.pipeline import Pipeline, node

from .nodes import build_sgs_series_refined


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=build_sgs_series_refined,
                inputs={"series_id": "params:sgs_selic_series_id"},
                outputs="refined_bcb_sgs_selic",
                name="build_refined_bcb_sgs_selic",
            ),
            node(
                func=build_sgs_series_refined,
                inputs={"series_id": "params:sgs_ipca_series_id"},
                outputs="refined_bcb_sgs_ipca",
                name="build_refined_bcb_sgs_ipca",
            ),
        ]
    )