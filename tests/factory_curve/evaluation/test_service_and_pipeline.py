from __future__ import annotations

import pandas as pd
import pytest
import yaml

from factory_curve.evaluation.pipeline import create_pipeline
from factory_curve.evaluation.service import CurveEvaluationService
from ml_ettj26.pipeline_registry import register_pipelines


class StubCalculator:
    result_keys = ("stub",)

    def calculate(self, context):
        return {
            "stub": pd.DataFrame(
                {
                    "methodology": [context.methodology],
                    "value": [1.0],
                }
            )
        }


def test_service_accepts_injected_calculators(evaluation_sample) -> None:
    service = CurveEvaluationService([StubCalculator()])
    result = service.evaluate(
        {"a": evaluation_sample["curve"], "b": evaluation_sample["curve"]},
        ltn_observations=evaluation_sample["ltn"],
        swap_observations=evaluation_sample["swaps"],
        calendar=evaluation_sample["calendar"],
        parameters=evaluation_sample["parameters"],
    )
    assert result["stub"]["methodology"].tolist() == ["a", "b"]


def test_service_rejects_invalid_calculator_contract(
    evaluation_sample,
) -> None:
    class InvalidCalculator(StubCalculator):
        def calculate(self, context):
            return {"wrong": pd.DataFrame()}

    service = CurveEvaluationService([InvalidCalculator()])
    with pytest.raises(ValueError, match="invalid result contract"):
        service.evaluate(
            {"a": evaluation_sample["curve"]},
            ltn_observations=evaluation_sample["ltn"],
            swap_observations=evaluation_sample["swaps"],
            calendar=evaluation_sample["calendar"],
            parameters=evaluation_sample["parameters"],
        )


def test_pipeline_contract() -> None:
    evaluation_pipeline = create_pipeline()
    assert len(evaluation_pipeline.nodes) == 1
    assert evaluation_pipeline.nodes[0].name == "evaluate_curve_methodologies"
    assert {
        "factory_curve_flat_forward_daily",
        "factory_curve_bootstrapping_daily",
        "factory_curve_nelson_siegel_daily",
        "factory_curve_svensson_daily",
        "factory_curve_kernel_ridge_daily",
        "mart_public_bonds_curve_inputs_dimension_batch",
        "refined_b3_swap_dipre",
        "refined_ref_calendar_br_market",
        "params:factory_curve_evaluation",
    } == evaluation_pipeline.inputs()
    assert len(evaluation_pipeline.outputs()) == 12


def test_registry_and_catalog_expose_evaluation_pipeline_and_outputs() -> None:
    pipelines = register_pipelines()
    assert "factory_curve_evaluation" in pipelines
    assert "factory_curve_evaluation_full" in pipelines
    assert len(pipelines["factory_curve_evaluation_full"].nodes) == 3

    with open("conf/base/catalog.yml", encoding="utf-8") as stream:
        catalog = yaml.safe_load(stream)
    evaluation_datasets = {
        name
        for name in catalog
        if name.startswith("factory_curve_evaluation_")
    }
    assert len(evaluation_datasets) == 12
    assert all(
        catalog[name]["filepath"].startswith(
            "data/08_reporting/factory_curve/evaluation/"
        )
        for name in evaluation_datasets
    )
