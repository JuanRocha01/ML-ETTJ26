from kedro.pipeline import Pipeline, node
from ml_ettj26.pipelines.trusted.b3.PriceReport.DI1.nodes import build_b3_di1_range_node

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=build_b3_di1_range_node,
                inputs=[
                    "raw_b3_price_report_zip_paths",
                    "trusted_ref_calendar_bd_index",
                    "trusted_b3_di1_instrument_master_prev",
                    "params:b3_di1_range",
                    "params:b3_price_report",
                ],
                outputs=[
                    "trusted_b3_di1_quotes_daily_by_month",
                    "trusted_b3_di1_lineage_by_month",
                    "trusted_b3_di1_instrument_master",
                ],
                name="trusted_build_b3_di1_range",
            )
        ]
    )
