from __future__ import annotations

from pathlib import Path

import yaml

from ml_ettj26.pipeline_registry import register_pipelines


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_registry_exposes_individual_and_combined_parametric_pipelines() -> None:
    pipelines = register_pipelines()

    assert "public_bonds_nelson_siegel" in pipelines
    assert "public_bonds_svensson" in pipelines
    assert "public_bonds_parametric_curves" in pipelines
    assert "public_bonds_nelson_siegel_curve_calculator" in pipelines
    assert "public_bonds_svensson_curve_calculator" in pipelines
    assert "public_bonds_parametric_curve_calculators" in pipelines
    assert "public_bonds_parametric_curves_full" in pipelines
    assert len(pipelines["public_bonds_parametric_curves"].nodes) == 2
    assert len(pipelines["public_bonds_parametric_curve_calculators"].nodes) == 4
    assert len(pipelines["public_bonds_parametric_curves_full"].nodes) == 6


def test_catalog_separates_statsmodels_pickles_by_method() -> None:
    catalog = yaml.safe_load(
        (PROJECT_ROOT / "conf" / "base" / "catalog.yml").read_text(
            encoding="utf-8"
        )
    )

    ns = catalog["public_bonds_nelson_siegel_models"]
    sv = catalog["public_bonds_svensson_models"]
    assert ns["path"].endswith("factory_curve/nelson_siegel")
    assert sv["path"].endswith("factory_curve/svensson")
    assert ns["dataset"]["type"] == "pickle.PickleDataset"
    assert sv["dataset"]["type"] == "pickle.PickleDataset"
    assert ns["filename_suffix"] == sv["filename_suffix"] == ".pkl"


def test_catalog_persists_dimensions_and_batched_curve_parquets() -> None:
    catalog = yaml.safe_load(
        (PROJECT_ROOT / "conf" / "base" / "catalog.yml").read_text(
            encoding="utf-8"
        )
    )

    ns_dimension = catalog[
        "public_bonds_nelson_siegel_parameter_dimension"
    ]
    sv_dimension = catalog["public_bonds_svensson_parameter_dimension"]
    ns_curves = catalog["public_bonds_nelson_siegel_curves"]
    sv_curves = catalog["public_bonds_svensson_curves"]

    assert ns_dimension["filepath"].endswith(
        "nelson_siegel/parameter_dimension.parquet"
    )
    assert sv_dimension["filepath"].endswith(
        "svensson/parameter_dimension.parquet"
    )
    assert ns_curves["filename_suffix"] == ".parquet"
    assert sv_curves["filename_suffix"] == ".parquet"
    assert ns_curves["save_lazily"] is True
    assert sv_curves["save_lazily"] is True
