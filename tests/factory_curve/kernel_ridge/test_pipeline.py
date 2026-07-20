from __future__ import annotations

from factory_curve.kernel_ridge.pipeline import create_pipeline


def test_kernel_ridge_pipeline_wires_tuning_models_and_curves() -> None:
    curve_pipeline = create_pipeline()

    assert curve_pipeline.inputs() == {
        "mart_public_bonds_cashflow_dimension",
        "mart_public_bonds_curve_inputs_dimension_batch",
        "params:kernel_ridge",
        "refined_ref_calendar_br_market",
    }
    assert curve_pipeline.outputs() == {
        "public_bonds_krr_curves",
        "public_bonds_krr_hyperparameter_search",
        "public_bonds_krr_model_dimension",
    }
    assert len(curve_pipeline.nodes) == 5
