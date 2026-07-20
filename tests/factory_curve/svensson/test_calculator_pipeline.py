from __future__ import annotations

from factory_curve.svensson.calculator_pipeline import create_pipeline


def test_svensson_calculator_pipeline_uses_model_partitions() -> None:
    curve_pipeline = create_pipeline()

    assert curve_pipeline.inputs() == {
        "public_bonds_svensson_models",
        "params:parametric_curve_calculator",
    }
    assert curve_pipeline.outputs() == {
        "public_bonds_svensson_parameter_dimension",
        "public_bonds_svensson_curves",
    }
    assert len(curve_pipeline.nodes) == 2
