# src/ml_ettj26/pipeline_registry.py
from __future__ import annotations

from kedro.pipeline import Pipeline

from ml_ettj26.pipelines.trusted.bcb_sgs.pipeline import create_pipeline as bcb_sgs_trusted


def register_pipelines() -> dict[str, Pipeline]:
    pipelines = {
        "trusted_bcb_sgs": bcb_sgs_trusted(),
    }

    # você pode definir o default
    pipelines["__default__"] = pipelines["trusted_bcb_sgs"]

    return pipelines
