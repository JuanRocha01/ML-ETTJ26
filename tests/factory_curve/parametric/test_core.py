from __future__ import annotations

import pickle

import numpy as np
import pandas as pd
import pytest
from scipy.optimize import OptimizeResult
from statsmodels.regression.linear_model import OLS

from factory_curve.nelson_siegel.model import NelsonSiegelSpecification
from factory_curve.parametric.core import (
    LARGE_PENALTY,
    CurveFitConfig,
    DailyCurveFitter,
    DifferentialEvolutionConfig,
    ModifiedDurationWeighting,
    ProfiledWLSObjective,
    ScipyDifferentialEvolution,
    fit_models_by_date,
    fit_wls,
    prepare_curve_inputs,
)


class FixedOptimizer:
    def __init__(self, lambdas: tuple[float, ...]) -> None:
        self._log_lambdas = np.log(np.asarray(lambdas))

    def optimize(self, objective, config) -> OptimizeResult:
        return OptimizeResult(
            x=self._log_lambdas,
            fun=objective(self._log_lambdas),
            success=True,
            message="fixed test optimizer",
            nit=1,
            nfev=1,
        )


def make_config(**overrides) -> CurveFitConfig:
    values = {
        "de": DifferentialEvolutionConfig(
            lambda_bounds=((0.1, 3.0),),
            popsize=5,
            maxiter=2,
            polish=False,
            init="latinhypercube",
        ),
        "min_observations": 4,
        "show_progress": False,
    }
    values.update(overrides)
    return CurveFitConfig(**values)


def make_daily_frame() -> pd.DataFrame:
    tenors = np.array([0.5, 1.0, 2.0, 4.0, 7.0, 10.0])
    design = NelsonSiegelSpecification().design_matrix(tenors, [0.7])
    rates = design @ np.array([0.11, -0.03, 0.02])
    rates += np.array([0.0, 1e-5, -2e-5, 1e-5, 0.0, -1e-5])
    return pd.DataFrame(
        {
            "ref_date": pd.Timestamp("2024-01-02"),
            "instrument_type": ["LTN"] * 3 + ["NTN-F"] * 3,
            "isin": [f"TEST{i}" for i in range(6)],
            "macaulay_duration": tenors,
            "market_ytm": rates,
            "market_pu": [900.0, 850.0, 800.0, 1000.0, 1050.0, 1100.0],
            "modified_duration": tenors / (1.0 + rates),
        }
    )


def test_modified_duration_weights_are_squared_and_normalized() -> None:
    weights = ModifiedDurationWeighting().calculate([2.0, 4.0])

    assert weights == pytest.approx([0.4, 1.6])
    assert np.mean(weights) == pytest.approx(1.0)


def test_modified_duration_weighting_rejects_non_positive_values() -> None:
    with pytest.raises(ValueError, match="strictly positive"):
        ModifiedDurationWeighting().calculate([0.0])


def test_equal_weight_wls_matches_statsmodels_ols() -> None:
    design = np.column_stack((np.ones(5), np.arange(5.0)))
    rates = np.array([1.0, 2.1, 2.9, 4.2, 5.0])

    wls = fit_wls(
        rates=rates,
        design_matrix=design,
        weights=np.ones(5),
        beta_names=("beta_0", "beta_1"),
    )
    ols = OLS(rates, design).fit()

    assert np.asarray(wls.params) == pytest.approx(np.asarray(ols.params))
    assert np.asarray(wls.resid) == pytest.approx(np.asarray(ols.resid))


def test_profiled_objective_matches_manual_weighted_mse() -> None:
    frame = make_daily_frame()
    specification = NelsonSiegelSpecification()
    weights = np.linspace(0.5, 1.5, len(frame))
    objective = ProfiledWLSObjective(
        specification=specification,
        tenors=frame["macaulay_duration"].to_numpy(),
        rates=frame["market_ytm"].to_numpy(),
        weights=weights,
        condition_number_limit=1e10,
    )

    actual = objective(np.log([0.7]))
    design = specification.design_matrix(
        frame["macaulay_duration"].to_numpy(),
        [0.7],
    )
    result = fit_wls(
        rates=frame["market_ytm"].to_numpy(),
        design_matrix=design,
        weights=weights,
        beta_names=specification.beta_names,
    )
    expected = np.average(np.square(result.resid), weights=weights)

    assert actual == pytest.approx(expected)


def test_profiled_objective_penalizes_rank_deficient_design() -> None:
    objective = ProfiledWLSObjective(
        specification=NelsonSiegelSpecification(),
        tenors=np.ones(5),
        rates=np.linspace(0.1, 0.2, 5),
        weights=np.ones(5),
        condition_number_limit=1e10,
    )

    assert objective(np.log([0.7])) == LARGE_PENALTY


def test_daily_fitter_returns_statsmodels_result_with_curve_metadata() -> None:
    frame = make_daily_frame()
    fitter = DailyCurveFitter(
        specification=NelsonSiegelSpecification(),
        config=make_config(),
        optimizer=FixedOptimizer((0.7,)),
    )

    result = fitter.fit(frame)

    assert result.curve_metadata["model_name"] == "nelson_siegel"
    assert result.curve_metadata["reference_date"] == "2024-01-02"
    assert result.curve_metadata["lambdas"]["lambda_1"] == pytest.approx(0.7)
    assert result.curve_metadata["warm_start"] is False
    assert result.curve_metadata["schema_version"] == 2
    assert result.curve_metadata["weighting_basis"] == "modified_duration"
    assert result.curve_metadata["final_cov_type"] == "HC3"
    assert result.cov_type == "HC3"
    assert result.curve_metadata["n_observations"] == 6
    assert list(result.params.index) == ["beta_0", "beta_1", "beta_2"]
    assert np.asarray(result.params) == pytest.approx(
        [0.11, -0.03, 0.02],
        abs=5e-4,
    )
    assert np.isfinite(result.pvalues).all()


def test_statsmodels_result_and_custom_metadata_survive_pickle_round_trip() -> None:
    result = DailyCurveFitter(
        specification=NelsonSiegelSpecification(),
        config=make_config(),
        optimizer=FixedOptimizer((0.7,)),
    ).fit(make_daily_frame())

    restored = pickle.loads(pickle.dumps(result))

    assert np.asarray(restored.params) == pytest.approx(
        np.asarray(result.params)
    )
    assert restored.curve_metadata == result.curve_metadata
    assert restored.summary() is not None


def test_prepare_curve_inputs_filters_pre_2020_and_invalid_rows() -> None:
    frame = pd.concat(
        [
            make_daily_frame().assign(ref_date="2019-12-31"),
            make_daily_frame(),
            make_daily_frame().assign(
                ref_date="2024-01-03",
                modified_duration=0.0,
            ),
        ],
        ignore_index=True,
    )

    selected = prepare_curve_inputs(frame, make_config())

    assert selected["ref_date"].dt.strftime("%Y-%m-%d").unique().tolist() == [
        "2024-01-02"
    ]


def test_prepare_curve_inputs_does_not_require_market_price() -> None:
    selected = prepare_curve_inputs(
        make_daily_frame().drop(columns="market_pu"),
        make_config(),
    )

    assert len(selected) == len(make_daily_frame())
    assert "market_pu" not in selected.columns


def test_fit_models_by_date_returns_one_partition_per_eligible_date() -> None:
    first = make_daily_frame()
    second = make_daily_frame().assign(ref_date="2024-01-03")
    pre_start = make_daily_frame().assign(ref_date="2019-12-31")

    models = fit_models_by_date(
        pd.concat([pre_start, second, first], ignore_index=True),
        specification=NelsonSiegelSpecification(),
        config=make_config(),
        optimizer=FixedOptimizer((0.7,)),
    )

    assert list(models) == ["2024-01-02", "2024-01-03"]
    assert all(hasattr(model, "curve_metadata") for model in models.values())


def test_de_configuration_uses_positive_lambda_bounds_in_log_space() -> None:
    config = DifferentialEvolutionConfig(lambda_bounds=((0.1, 2.0),))

    assert config.log_bounds[0] == pytest.approx((np.log(0.1), np.log(2.0)))

    with pytest.raises(ValueError, match="0 < lower < upper"):
        DifferentialEvolutionConfig(lambda_bounds=((0.0, 2.0),))


def test_scipy_de_adapter_finds_log_space_quadratic_minimum() -> None:
    target = np.log(0.8)
    config = DifferentialEvolutionConfig(
        lambda_bounds=((0.1, 2.0),),
        popsize=8,
        maxiter=40,
        tol=1e-9,
        polish=True,
        init="sobol",
        seed=7,
    )

    result = ScipyDifferentialEvolution().optimize(
        lambda z: float((z[0] - target) ** 2),
        config,
    )

    assert np.exp(result.x[0]) == pytest.approx(0.8, rel=1e-5)
