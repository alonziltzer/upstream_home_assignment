from pipeline.config import DATA_LAKE_PATH
from pipeline.dagster.resources import spark_session_resource
from pipeline.assets.bronze import bronze_asset
from pipeline.assets.gold.vin_last_state_report import (
    vin_last_state_report_asset,
)
from pipeline.assets.gold.top_10_report import (
    top_10_fastest_vehicles_per_date_hour_report_asset,
)
from pipeline.assets.silver import silver_asset
from dagster import Definitions
from pipeline.dagster.spark_io_manager import spark_parquet_io_manager
from pipeline.assets.bonus import bonus_asset

defs = Definitions(
    assets=[
        bronze_asset,
        silver_asset,
        vin_last_state_report_asset,
        top_10_fastest_vehicles_per_date_hour_report_asset,
        bonus_asset,
    ],
    resources={
        "spark_session_resource": spark_session_resource,
        "io_manager": spark_parquet_io_manager.configured(
            {"base_path": DATA_LAKE_PATH}
        ),
    },
)
