from pipeline.dagster.resources import spark_session_resource
from pipeline.assets.bronze import bronze_asset
from pipeline.assets.gold import (
    gold_vin_last_state_report_asset,
    gold_top_10_fastest_vehicles_per_date_hour_report_asset,
)
from pipeline.assets.silver import silver_asset
from dagster import Definitions
from pipeline.dagster.spark_io_manager import spark_parquet_io_manager

defs = Definitions(
    assets=[
        bronze_asset,
        silver_asset,
        gold_vin_last_state_report_asset,
        gold_top_10_fastest_vehicles_per_date_hour_report_asset,
    ],
    resources={
        "spark_session_resource": spark_session_resource,
        "io_manager": spark_parquet_io_manager.configured(
            {"base_path": "/Users/aziltzer/projects/upstream_home_assignment/data_lake"}
        ),
    },
)
