from factory_curve.data_treatment.pipeline import create_pipeline


def test_pipeline_contract() -> None:
    curve_pipeline = create_pipeline()

    assert {node.name for node in curve_pipeline.nodes} == {
        "data_treatment",
        "register_factory_curve_duckdb_views",
    }
    assert "public_bonds_flat_forward_curves" in curve_pipeline.inputs()
    assert "public_bonds_bootstrapped_curves" in curve_pipeline.inputs()
    assert "public_bonds_nelson_siegel_curves" in curve_pipeline.inputs()
    assert "public_bonds_svensson_curves" in curve_pipeline.inputs()
    assert "public_bonds_krr_curves" in curve_pipeline.inputs()
    assert "factory_curve_flat_forward_daily" in curve_pipeline.outputs()
    assert "factory_curve_bootstrapping_daily" in curve_pipeline.outputs()
