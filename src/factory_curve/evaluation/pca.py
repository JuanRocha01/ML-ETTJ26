from __future__ import annotations

from itertools import permutations

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA

from .contracts import EvaluationContext


def _factor_templates(size: int) -> dict[str, np.ndarray]:
    axis = np.linspace(-1.0, 1.0, size)
    templates = {
        "level": np.ones(size),
        "slope": axis,
        "curvature": 1.0 - 3.0 * axis**2,
    }
    return {
        name: values / np.linalg.norm(values)
        for name, values in templates.items()
    }


def _assign_factors(
    components: np.ndarray,
) -> dict[str, tuple[int, float]]:
    templates = _factor_templates(components.shape[1])
    names = tuple(templates)
    correlations = np.asarray(
        [
            [
                np.dot(component / np.linalg.norm(component), templates[name])
                for component in components
            ]
            for name in names
        ]
    )
    best = max(
        permutations(range(components.shape[0]), len(names)),
        key=lambda order: sum(
            abs(correlations[row, component])
            for row, component in enumerate(order)
        ),
    )
    return {
        name: (
            component,
            1.0 if correlations[row, component] >= 0.0 else -1.0,
        )
        for row, (name, component) in enumerate(zip(names, best, strict=True))
    }


class DailyPCACalculator:
    """Three-factor PCA of daily curve changes."""

    result_keys = ("pca_scores_daily", "pca_loadings")

    def calculate(
        self,
        context: EvaluationContext,
    ) -> dict[str, pd.DataFrame]:
        step_bd = int(context.parameters.get("pca_tenor_step_bd", 21))
        levels = context.curve.selected_tenors(step_bd)
        changes = levels.diff().iloc[1:]
        complete = changes.dropna(axis=0, how="any")
        if len(complete) < 4:
            raise ValueError(
                f"{context.methodology} has insufficient complete daily "
                "changes for three-factor PCA"
            )

        model = PCA(n_components=3, svd_solver="full")
        raw_scores = model.fit_transform(complete.to_numpy())
        assignments = _assign_factors(model.components_)
        score_rows: list[dict] = []
        loading_rows: list[dict] = []
        for factor, (component_index, sign) in assignments.items():
            explained = float(
                model.explained_variance_ratio_[component_index]
            )
            for ref_date, score in zip(
                complete.index,
                raw_scores[:, component_index],
                strict=True,
            ):
                score_rows.append(
                    {
                        "methodology": context.methodology,
                        "ref_date": ref_date,
                        "factor": factor,
                        "score": float(sign * score),
                        "explained_variance_ratio": explained,
                    }
                )
            for tenor_bd, loading in zip(
                complete.columns,
                model.components_[component_index],
                strict=True,
            ):
                loading_rows.append(
                    {
                        "methodology": context.methodology,
                        "factor": factor,
                        "tenor_bd": int(tenor_bd),
                        "loading": float(sign * loading),
                        "explained_variance_ratio": explained,
                    }
                )
        return {
            "pca_scores_daily": pd.DataFrame(score_rows).sort_values(
                ["methodology", "ref_date", "factor"]
            ).reset_index(drop=True),
            "pca_loadings": pd.DataFrame(loading_rows).sort_values(
                ["methodology", "factor", "tenor_bd"]
            ).reset_index(drop=True),
        }
