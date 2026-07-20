from __future__ import annotations

from factory_curve.svensson.pipeline import create_pipeline


def test_svensson_pipeline_uses_separate_partitioned_output() -> None:
    curve_pipeline = create_pipeline()

    assert curve_pipeline.inputs() == {
        "mart_public_bonds_curve_inputs_dimension_batch",
        "params:svensson",
    }
    assert curve_pipeline.outputs() == {"public_bonds_svensson_models"}
    assert len(curve_pipeline.nodes) == 1
