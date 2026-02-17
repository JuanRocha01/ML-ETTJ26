from __future__ import annotations

from kedro.pipeline import Pipeline, node

from ml_ettj26.pipelines.trusted.bcb_sgs.nodes import (
    build_bcb_sgs_series_meta_trusted,
    build_bcb_sgs_points_trusted,
    validate_bcb_sgs_series_meta,
    validate_bcb_sgs_points,
)


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=build_bcb_sgs_series_meta_trusted,
                inputs="params:bcb_sgs",
                outputs="bcb_sgs_series_meta_trusted__pre",
                name="trusted_bcb_sgs_build_meta",
            ),
            node(
                func=validate_bcb_sgs_series_meta,
                inputs="bcb_sgs_series_meta_trusted__pre",
                outputs="bcb_sgs_series_meta_trusted",
                name="trusted_bcb_sgs_validate_meta",
            ),
            node(
                func=build_bcb_sgs_points_trusted,
                inputs="params:bcb_sgs",
                outputs="bcb_sgs_points_trusted__pre",
                name="trusted_bcb_sgs_build_points",
            ),
            node(
                func=validate_bcb_sgs_points,
                inputs="bcb_sgs_points_trusted__pre",
                outputs="bcb_sgs_points_trusted",
                name="trusted_bcb_sgs_validate_points",
            ),
        ]
    )
