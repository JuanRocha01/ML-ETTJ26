# src/ml_ettj26/pipelines/data_engineering/pipeline.py
from kedro.pipeline import Pipeline, node, pipeline

from ml_ettj26.nodes.extraction import (
    extract_bcb_rates,
    extract_anbima_bonds,
    extract_b3_di_futures,
)

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=extract_bcb_rates,
            inputs="params:bcb",
            outputs="bcb_rates",
            name="extract_bcb_rates",
        ),
        node(
            func=extract_anbima_bonds,
            inputs="params:anbima",
            outputs="anbima_treasury_bonds_raw",
            name="extract_anbima_bonds",
        ),
        node(
            func=extract_b3_di_futures,
            inputs="params:b3_di",
            outputs="b3_di_futures_raw",
            name="extract_b3_di_futures",
        ),
    ])


