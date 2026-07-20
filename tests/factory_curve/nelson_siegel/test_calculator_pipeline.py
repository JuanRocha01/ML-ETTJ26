from __future__ import annotations

from factory_curve.nelson_siegel.calculator_pipeline import create_pipeline


def test_nelson_siegel_calculator_pipeline_uses_model_partitions() -> None:
    curve_pipeline = create_pipeline()

    assert curve_pipeline.inputs() == {
        "public_bonds_nelson_siegel_models",
        "params:parametric_curve_calculator",
    }
    assert curve_pipeline.outputs() == {
        "public_bonds_nelson_siegel_parameter_dimension",
        "public_bonds_nelson_siegel_curves",
    }
    assert len(curve_pipeline.nodes) == 2
