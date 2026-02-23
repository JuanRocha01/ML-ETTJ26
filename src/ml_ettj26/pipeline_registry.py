# src/ml_ettj26/pipeline_registry.py
from __future__ import annotations

from kedro.pipeline import Pipeline

from ml_ettj26.pipelines.trusted.bcb_sgs.pipeline import create_pipeline as bcb_sgs_trusted
from ml_ettj26.pipelines.trusted.bcb_demab.pipeline import create_pipeline as demab_trusted
from ml_ettj26.pipelines.trusted.anbima.pipeline import create_pipeline as anbima_calendar_trusted
from ml_ettj26.pipelines.trusted.b3.PriceReport.DI1 import pipeline as b3_di1_pipeline



def register_pipelines() -> dict[str, Pipeline]:
    trusted_bcb_sgs = bcb_sgs_trusted()
    trusted_bcb_demab = demab_trusted()
    trusted_anbima_calendar = anbima_calendar_trusted()
    trusted_b3_di1 = b3_di1_pipeline.create_pipeline()
    trusted_all = trusted_bcb_sgs + trusted_bcb_demab

    pipelines = {
        "trusted_anbima_calendar": trusted_anbima_calendar,
        "trusted_bcb_sgs": trusted_bcb_sgs,
        "trusted_bcb_demab": trusted_bcb_demab,
        "trusted_b3_di1": trusted_b3_di1,
        "trusted_all": trusted_all,
    }

    # você pode definir o default
    pipelines["__default__"] = pipelines["trusted_all"]

    return pipelines
