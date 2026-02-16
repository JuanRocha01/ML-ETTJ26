from __future__ import annotations

from kedro.pipeline import Pipeline, node

from ml_ettj26.pipelines.trusted.bcb_sgs.nodes import (
    build_bcb_sgs_trusted,
    validate_bcb_sgs_trusted,
)


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=build_bcb_sgs_trusted,
                inputs="params:bcb_sgs",
                outputs="bcb_sgs_daily_trusted__pre",
                name="trusted_bcb_sgs_build",
            ),
            node(
                func=validate_bcb_sgs_trusted,
                inputs="bcb_sgs_daily_trusted__pre",
                outputs="bcb_sgs_daily_trusted",
                name="trusted_bcb_sgs_validate",
            ),
        ]
    )
