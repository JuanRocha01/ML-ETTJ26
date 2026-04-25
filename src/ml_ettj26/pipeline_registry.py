# src/ml_ettj26/pipeline_registry.py
from __future__ import annotations

from kedro.pipeline import Pipeline

from ml_ettj26.pipelines.trusted.bcb_sgs.pipeline import create_pipeline as bcb_sgs_trusted
from ml_ettj26.pipelines.trusted.bcb_demab.pipeline import create_pipeline as demab_trusted
from ml_ettj26.pipelines.trusted.anbima.pipeline import create_pipeline as anbima_calendar_trusted
from ml_ettj26.pipelines.trusted.b3.PriceReport.DI1.pipeline import create_pipeline as b3_di1_pipeline
from ml_ettj26.pipelines.trusted.b3.DerivativeMarket.SwapMarketRates.DIxPRE.pipeline import create_pipeline as b3_swap_dixpre_pipeline

from ml_ettj26.pipelines.refined.calendar.pipeline import create_pipeline as refined_calendar_pipeline
from ml_ettj26.pipelines.refined.bcb_sgs import create_pipeline as create_bcb_sgs_refined_pipeline
from ml_ettj26.pipelines.refined.bcb_demab import create_pipeline as create_bcb_demab_refined_pipeline
from ml_ettj26.pipelines.refined.b3_di1 import create_pipeline as create_b3_forwards_di1_refined_pipeline
from ml_ettj26.pipelines.refined.b3_swaps.b3_dixpre import create_pipeline as b3_swap_dixpre_pipeline

def register_pipelines() -> dict[str, Pipeline]:
    trusted_bcb_sgs = bcb_sgs_trusted()
    trusted_bcb_demab = demab_trusted()
    trusted_anbima_calendar = anbima_calendar_trusted()
    trusted_b3_di1 = b3_di1_pipeline()
    trusted_b3_swap_dixpre = b3_swap_dixpre_pipeline()

    trusted_all = trusted_bcb_sgs + trusted_bcb_demab

    refined_calendar = refined_calendar_pipeline()
    refined_bcb_sgs = create_bcb_sgs_refined_pipeline()
    refined_bcb_demab = create_bcb_demab_refined_pipeline()
    refined_b3_forward_di1 = create_b3_forwards_di1_refined_pipeline()
    refined_b3_swap_dixpre = b3_swap_dixpre_pipeline()

    pipelines = {
        "trusted_all": trusted_all,

        "trusted_anbima_calendar": trusted_anbima_calendar,
        "trusted_bcb_sgs": trusted_bcb_sgs,
        "trusted_bcb_demab": trusted_bcb_demab,
        "trusted_b3_di1": trusted_b3_di1,
        "trusted_b3_swap_dixpre": trusted_b3_swap_dixpre,

        "refined_calendar": refined_calendar,
        "refined_bcb_sgs": refined_bcb_sgs,
        "refined_bcb_demab": refined_bcb_demab,
        "refined_b3_forward_di1": refined_b3_forward_di1,
        "refined_b3_swap_dixpre": refined_b3_swap_dixpre,
    }

    # você pode definir o default
    pipelines["__default__"] = pipelines["trusted_all"]

    return pipelines
