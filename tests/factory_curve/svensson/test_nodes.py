from __future__ import annotations

import pandas as pd

from factory_curve.svensson import nodes


def test_svensson_node_builds_two_lambda_configuration(monkeypatch) -> None:
    captured = {}

    def fake_fit(curve_inputs, *, specification, config):
        captured["specification"] = specification
        captured["config"] = config
        return {"2024-01-02": object()}

    monkeypatch.setattr(nodes, "fit_models_by_date", fake_fit)
    result = nodes.fit_svensson_models(
        pd.DataFrame(),
        {
            "start_date": "2020-01-01",
            "min_lambda_ratio": 1.3,
            "de": {
                "lambda_bounds": [[0.4, 2.0], [0.1, 0.3]],
            },
        },
    )

    assert set(result) == {"2024-01-02"}
    assert captured["specification"].min_lambda_ratio == 1.3
    assert len(captured["config"].de.lambda_bounds) == 2
