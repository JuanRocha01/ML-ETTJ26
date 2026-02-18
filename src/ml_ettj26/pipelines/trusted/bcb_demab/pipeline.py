from kedro.pipeline import Pipeline, node
from ml_ettj26.pipelines.trusted.bcb_demab.nodes import (build_demab_instruments, build_demab_quotes,
                                                          validate_instruments, validate_quotes,
                                                           build_demab_quotes_partitioned, validate_demab_quotes_partitioned)

def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(func=build_demab_instruments, 
                 inputs="params:bcb_demab", 
                 outputs=  "bcb_demab_instruments_trusted__pre", 
                 name="trusted_bcb_demab_build_dim"),

            node(func=validate_instruments,
                 inputs="bcb_demab_instruments_trusted__pre", 
                 outputs="bcb_demab_instruments_trusted", 
                 name="trusted_bcb_demab_validate_dim"),

            node(
                func=build_demab_quotes_partitioned,
                inputs="params:bcb_demab",
                outputs="bcb_demab_quotes_daily_trusted__pre",
                name="trusted_bcb_demab_build_fact"),

            node(func=validate_demab_quotes_partitioned, 
                 inputs="bcb_demab_quotes_daily_trusted__pre", 
                 outputs="bcb_demab_quotes_daily_trusted", 
                 name="trusted_bcb_demab_validate_fact"),
        ]
    )
