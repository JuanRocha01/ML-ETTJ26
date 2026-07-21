from __future__ import annotations

from factory_curve.flat_forward.pipeline import create_pipeline
from ml_ettj26.pipeline_registry import register_pipelines


def test_flat_forward_pipeline_uses_expected_catalog_datasets() -> None:
    curve_pipeline = create_pipeline()

    assert curve_pipeline.inputs() == {
        "mart_public_bonds_curve_inputs_dimension_batch",
        "params:flat_forward",
    }
    assert curve_pipeline.outputs() == {"public_bonds_flat_forward_curves"}
    assert len(curve_pipeline.nodes) == 1


def test_registry_uses_flat_forward_pipeline_name() -> None:
    pipelines = register_pipelines()

    assert "public_bonds_flat_forward" in pipelines
    assert "public_bonds_bootstrapping" in pipelines
    assert pipelines["public_bonds_flat_forward"] is not pipelines[
        "public_bonds_bootstrapping"
    ]
