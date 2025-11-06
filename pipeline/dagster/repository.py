from dagster import Definitions

from pipeline.config import DATA_LAKE_PATH
from pipeline.dagster.spark_io_manager import (
    spark_parquet_io_manager,
)
from pipeline.assets.bronze import bronze_asset
from pipeline.assets.silver import silver_asset
from pipeline.assets.gold import gold_asset
from pipeline.dagster.resources import spark_session_resource

definitions = Definitions(
    assets=[bronze_asset, silver_asset, gold_asset],
    resources={
        "spark_session_resource": spark_session_resource,
        "io_manager": spark_parquet_io_manager.configured(base_path=DATA_LAKE_PATH),
    },
)
