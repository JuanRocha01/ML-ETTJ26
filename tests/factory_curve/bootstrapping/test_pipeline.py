from __future__ import annotations

from factory_curve.bootstrapping.pipeline import create_pipeline


def test_bootstrapping_pipeline_uses_expected_catalog_datasets() -> None:
    curve_pipeline = create_pipeline()

    assert curve_pipeline.inputs() == {
        "mart_public_bonds_curve_inputs_dimension_batch",
        "params:bootstrapping",
    }
    assert curve_pipeline.outputs() == {"public_bonds_flat_forward_curves"}
    assert len(curve_pipeline.nodes) == 1
