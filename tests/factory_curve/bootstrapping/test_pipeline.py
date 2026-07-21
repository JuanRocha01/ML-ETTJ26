from __future__ import annotations

from factory_curve.bootstrapping.pipeline import create_pipeline
from ml_ettj26.pipeline_registry import register_pipelines


def test_bootstrapping_pipeline_reuses_cashflow_dimension() -> None:
    curve_pipeline = create_pipeline()

    assert curve_pipeline.inputs() == {
        "mart_public_bonds_curve_inputs_dimension_batch",
        "mart_public_bonds_cashflow_dimension",
        "refined_ref_calendar_br_market",
        "params:bootstrapping",
    }
    assert curve_pipeline.outputs() == {
        "public_bonds_bootstrapped_curves",
        "public_bonds_bootstrapping_diagnostics",
    }


def test_registry_exposes_true_bootstrapping_and_flat_forward_separately() -> None:
    pipelines = register_pipelines()

    assert "public_bonds_bootstrapping" in pipelines
    assert "public_bonds_flat_forward" in pipelines
