# src/ml_ettj26/pipeline_registry.py
from __future__ import annotations

from kedro.pipeline import Pipeline

from ml_ettj26.pipelines.trusted.bcb_sgs.pipeline import create_pipeline as bcb_sgs_trusted
from ml_ettj26.pipelines.trusted.bcb_demab.pipeline import create_pipeline as demab_trusted
from ml_ettj26.pipelines.trusted.anbima.pipeline import create_pipeline as anbima_calendar_trusted
from ml_ettj26.pipelines.trusted.b3.PriceReport.DI1.pipeline import create_pipeline as b3_di1_pipeline
from ml_ettj26.pipelines.trusted.b3.DerivativeMarket.SwapMarketRates.DIxPRE.pipeline import (
    create_pipeline as create_b3_swap_dixpre_trusted_pipeline,
)
from ml_ettj26.pipelines.trusted.view_duckdb.pipeline import create_pipeline as register_trusted_view

from ml_ettj26.pipelines.refined.calendar.pipeline import create_pipeline as refined_calendar_pipeline
from ml_ettj26.pipelines.refined.bcb_sgs import create_pipeline as create_bcb_sgs_refined_pipeline
from ml_ettj26.pipelines.refined.bcb_demab import create_pipeline as create_bcb_demab_refined_pipeline
from ml_ettj26.pipelines.refined.b3_di1 import create_pipeline as create_b3_forwards_di1_refined_pipeline
from ml_ettj26.pipelines.refined.b3_swaps.b3_dixpre import (
    create_pipeline as create_b3_swap_dixpre_refined_pipeline,
)
from ml_ettj26.pipelines.refined.view_duckdb.pipeline import create_pipeline as register_refined_view

from ml_ettj26.pipelines.curve_factory.public_bonds_mart import pipeline as public_bonds_mart_pipeline
from ml_ettj26.pipelines.curve_factory.public_bonds_mart import pipeline_batch as public_bonds_mart_batch_pipeline
from ml_ettj26.pipelines.curve_factory.public_bonds_mart import pipeline_dimension_batch as public_bonds_mart_dimension_batch_pipeline
from ml_ettj26.pipelines.curve_factory.public_bonds_cashflows import pipeline as public_bonds_cashflows_pipeline
from factory_curve.bootstrapping import pipeline as bootstrapping_pipeline
from factory_curve.kernel_ridge import pipeline as kernel_ridge_pipeline
from factory_curve.nelson_siegel import pipeline as nelson_siegel_pipeline
from factory_curve.nelson_siegel import calculator_pipeline as nelson_siegel_calculator_pipeline
from factory_curve.svensson import pipeline as svensson_pipeline
from factory_curve.svensson import calculator_pipeline as svensson_calculator_pipeline


def register_pipelines() -> dict[str, Pipeline]:
    trusted_bcb_sgs = bcb_sgs_trusted()
    trusted_bcb_demab = demab_trusted()
    trusted_anbima_calendar = anbima_calendar_trusted()
    trusted_b3_di1 = b3_di1_pipeline()
    trusted_b3_swap_dixpre = create_b3_swap_dixpre_trusted_pipeline()
    trusted_register_view = register_trusted_view()

    refined_calendar = refined_calendar_pipeline()
    refined_bcb_sgs = create_bcb_sgs_refined_pipeline()
    refined_bcb_demab = create_bcb_demab_refined_pipeline()
    refined_b3_forward_di1 = create_b3_forwards_di1_refined_pipeline()
    refined_b3_swap_dixpre = create_b3_swap_dixpre_refined_pipeline()
    refined_register_view = register_refined_view()

    public_bonds_mart = public_bonds_mart_pipeline.create_pipeline()
    public_bonds_mart_batch = public_bonds_mart_batch_pipeline.create_pipeline()
    public_bonds_mart_dimension_batch = public_bonds_mart_dimension_batch_pipeline.create_pipeline()
    public_bonds_cashflows = public_bonds_cashflows_pipeline.create_pipeline()
    public_bonds_bootstrapping = bootstrapping_pipeline.create_pipeline()
    public_bonds_kernel_ridge = kernel_ridge_pipeline.create_pipeline()
    public_bonds_nelson_siegel = nelson_siegel_pipeline.create_pipeline()
    public_bonds_svensson = svensson_pipeline.create_pipeline()
    public_bonds_nelson_siegel_curve_calculator = (
        nelson_siegel_calculator_pipeline.create_pipeline()
    )
    public_bonds_svensson_curve_calculator = (
        svensson_calculator_pipeline.create_pipeline()
    )
    public_bonds_parametric_curve_calculators = (
        public_bonds_nelson_siegel_curve_calculator
        + public_bonds_svensson_curve_calculator
    )


    pipelines = {
        "trusted_anbima_calendar": trusted_anbima_calendar,
        "trusted_bcb_sgs": trusted_bcb_sgs,
        "trusted_bcb_demab": trusted_bcb_demab,
        "trusted_b3_di1": trusted_b3_di1,
        "trusted_b3_swap_dixpre": trusted_b3_swap_dixpre,
        "trusted_register_view": trusted_register_view,

        "refined_calendar": refined_calendar,
        "refined_bcb_sgs": refined_bcb_sgs,
        "refined_bcb_demab": refined_bcb_demab,
        "refined_b3_forward_di1": refined_b3_forward_di1,
        "refined_b3_swap_dixpre": refined_b3_swap_dixpre,
        "refined_register_view": refined_register_view,

        "public_bonds_cashflows": public_bonds_cashflows,

        "public_bonds_mart": public_bonds_mart,
        "public_bonds_mart_batch": public_bonds_mart_batch,
        "public_bonds_mart_dimension_batch": public_bonds_mart_dimension_batch,
        "public_bonds_bootstrapping": public_bonds_bootstrapping,
        "public_bonds_kernel_ridge": public_bonds_kernel_ridge,
        "public_bonds_nelson_siegel": public_bonds_nelson_siegel,
        "public_bonds_svensson": public_bonds_svensson,
        "public_bonds_parametric_curves": (
            public_bonds_nelson_siegel + public_bonds_svensson
        ),
        "public_bonds_nelson_siegel_curve_calculator": (
            public_bonds_nelson_siegel_curve_calculator
        ),
        "public_bonds_svensson_curve_calculator": (
            public_bonds_svensson_curve_calculator
        ),
        "public_bonds_parametric_curve_calculators": (
            public_bonds_parametric_curve_calculators
        ),
        "public_bonds_parametric_curves_full": (
            public_bonds_nelson_siegel
            + public_bonds_svensson
            + public_bonds_parametric_curve_calculators
        ),
        
    }

    return pipelines
