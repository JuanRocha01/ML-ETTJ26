from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from factory_curve.nelson_siegel.calculator import (
    NelsonSiegelCurveCalculator,
)
from factory_curve.nelson_siegel.model import NelsonSiegelSpecification
from factory_curve.parametric.calculator import (
    CurveBatchPartitionBuilder,
    CurveCalculationConfig,
    ModelDimensionBuilder,
    load_model_partition,
)
from factory_curve.svensson.calculator import SvenssonCurveCalculator
from factory_curve.svensson.model import SvenssonSpecification


def make_model(
    *,
    model_name: str,
    beta_names: tuple[str, ...],
    betas: tuple[float, ...],
    lambda_names: tuple[str, ...],
    lambdas: tuple[float, ...],
    reference_date: str = "2024-01-02",
):
    params = pd.Series(betas, index=beta_names, dtype="float64")
    pvalues = pd.Series(
        np.linspace(0.01, 0.04, len(beta_names)),
        index=beta_names,
        dtype="float64",
    )
    return SimpleNamespace(
        params=params,
        pvalues=pvalues,
        cov_type="HC3",
        aic=-100.0,
        bic=-95.0,
        rsquared=0.99,
        rsquared_adj=0.98,
        curve_metadata={
            "schema_version": 2,
            "model_name": model_name,
            "reference_date": reference_date,
            "lambdas": dict(zip(lambda_names, lambdas, strict=True)),
            "peak_maturities": {
                f"peak_maturity_{index}": 1.7932821329 / value
                for index, value in enumerate(lambdas, start=1)
            },
            "rmse": 0.001,
            "weighted_rmse": 0.0008,
            "max_abs_error": 0.002,
            "n_observations": 12,
            "matrix_rank": len(beta_names),
            "condition_number": 25.0,
            "weighting_basis": "modified_duration",
            "optimizer_success": True,
            "optimizer_nit": 10,
            "optimizer_nfev": 120,
        },
    )


def make_ns_model(reference_date: str = "2024-01-02"):
    specification = NelsonSiegelSpecification()
    return make_model(
        model_name=specification.name,
        beta_names=specification.beta_names,
        betas=(0.10, -0.02, 0.03),
        lambda_names=specification.lambda_names,
        lambdas=(0.7,),
        reference_date=reference_date,
    )


def make_svensson_model(reference_date: str = "2024-01-02"):
    specification = SvenssonSpecification()
    return make_model(
        model_name=specification.name,
        beta_names=specification.beta_names,
        betas=(0.10, -0.02, 0.03, 0.01),
        lambda_names=specification.lambda_names,
        lambdas=(0.8, 0.2),
        reference_date=reference_date,
    )


def small_config(batch_size: int = 2) -> CurveCalculationConfig:
    return CurveCalculationConfig(
        max_years=1,
        business_days_per_year=2,
        model_batch_size=batch_size,
        show_progress=False,
    )


def test_default_grid_contains_every_business_day_through_twenty_years() -> None:
    config = CurveCalculationConfig()

    assert config.grid_size == 20 * 252 == 5040


def test_nelson_siegel_calculator_matches_model_loadings() -> None:
    model = make_ns_model()
    calculator = NelsonSiegelCurveCalculator(small_config())

    curve = calculator.calculate_curve(model)

    expected_design = NelsonSiegelSpecification().design_matrix(
        [0.5, 1.0],
        [0.7],
    )
    expected_rates = expected_design @ np.array([0.10, -0.02, 0.03])
    assert curve["tenor_bd"].tolist() == [1, 2]
    assert curve["tenor_years"].tolist() == pytest.approx([0.5, 1.0])
    assert curve["fitted_rate"].to_numpy() == pytest.approx(expected_rates)
    assert curve["ref_date"].dt.strftime("%Y-%m-%d").unique().tolist() == [
        "2024-01-02"
    ]


def test_svensson_calculator_matches_model_loadings() -> None:
    model = make_svensson_model()
    calculator = SvenssonCurveCalculator(small_config())

    curve = calculator.calculate_curve(model)

    expected_design = SvenssonSpecification().design_matrix(
        [0.5, 1.0],
        [0.8, 0.2],
    )
    expected_rates = expected_design @ np.array(
        [0.10, -0.02, 0.03, 0.01]
    )
    assert curve["fitted_rate"].to_numpy() == pytest.approx(expected_rates)


def test_parameter_dimension_contains_coefficients_pvalues_lambdas_and_rmse() -> None:
    calculator = NelsonSiegelCurveCalculator(small_config())
    partitions = {
        "2024-01-03": lambda: make_ns_model("2024-01-03"),
        "2024-01-02": lambda: make_ns_model("2024-01-02"),
    }

    dimension = ModelDimensionBuilder(calculator).build(partitions)

    assert dimension["ref_date"].dt.strftime("%Y-%m-%d").tolist() == [
        "2024-01-02",
        "2024-01-03",
    ]
    assert dimension.loc[0, "beta_0"] == pytest.approx(0.10)
    assert dimension.loc[0, "pvalue_beta_0"] == pytest.approx(0.01)
    assert dimension.loc[0, "lambda_1"] == pytest.approx(0.7)
    assert dimension.loc[0, "rmse"] == pytest.approx(0.001)
    assert dimension.loc[0, "weighted_rmse"] == pytest.approx(0.0008)
    assert dimension.loc[0, "cov_type"] == "HC3"


def test_curve_partition_builder_groups_dates_and_calculates_lazily() -> None:
    calculator = NelsonSiegelCurveCalculator(small_config(batch_size=2))
    load_count = {"value": 0}

    def loader(reference_date: str):
        def load():
            load_count["value"] += 1
            return make_ns_model(reference_date)

        return load

    partitions = {
        "2024-01-02": loader("2024-01-02"),
        "2024-01-03": loader("2024-01-03"),
        "2024-01-04": loader("2024-01-04"),
    }

    batches = CurveBatchPartitionBuilder(calculator).build(partitions)

    assert list(batches) == ["batch_00000", "batch_00001"]
    assert load_count["value"] == 0

    first_batch = batches["batch_00000"]()
    second_batch = batches["batch_00001"]()

    assert load_count["value"] == 3
    assert len(first_batch) == 2 * calculator.config.grid_size
    assert len(second_batch) == calculator.config.grid_size
    assert first_batch["ref_date"].nunique() == 2
    assert second_batch["ref_date"].nunique() == 1


def test_model_loader_accepts_lazy_and_eager_partitions() -> None:
    model = make_ns_model()

    assert load_model_partition(model) is model
    assert load_model_partition(lambda: model) is model


def test_calculator_rejects_model_from_another_specification() -> None:
    calculator = NelsonSiegelCurveCalculator(small_config())

    with pytest.raises(ValueError, match="does not match"):
        calculator.calculate_curve(make_svensson_model())


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_years": 0},
        {"business_days_per_year": 0},
        {"model_batch_size": 0},
    ],
)
def test_curve_calculation_config_rejects_invalid_values(kwargs) -> None:
    with pytest.raises(ValueError, match="strictly positive"):
        CurveCalculationConfig(**kwargs)
