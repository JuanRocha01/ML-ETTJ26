from __future__ import annotations

import pandas as pd

from factory_curve.nelson_siegel import nodes


def test_nelson_siegel_node_builds_one_lambda_configuration(monkeypatch) -> None:
    captured = {}

    def fake_fit(curve_inputs, *, specification, config):
        captured["specification"] = specification
        captured["config"] = config
        return {"2024-01-02": object()}

    monkeypatch.setattr(nodes, "fit_models_by_date", fake_fit)
    result = nodes.fit_nelson_siegel_models(
        pd.DataFrame(),
        {
            "start_date": "2020-01-01",
            "de": {"lambda_bounds": [[0.1, 2.0]]},
        },
    )

    assert set(result) == {"2024-01-02"}
    assert captured["specification"].name == "nelson_siegel"
    assert len(captured["config"].de.lambda_bounds) == 1
