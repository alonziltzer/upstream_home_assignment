from dagster import IOManager
from pyspark.sql import SparkSession
import os
from dagster import io_manager, AssetMaterialization, MetadataValue


class SparkParquetIOManager(IOManager):
    def __init__(self, base_path: str, spark: SparkSession):
        self.base_path = base_path
        self.spark = spark
        os.makedirs(self.base_path, exist_ok=True)

    def handle_output(self, context, obj):
        asset_name = (
            context.asset_key.path[-1] if context.asset_key else context.step_key
        )
        path = _get_path(self, asset_name, context)
        partition_cols = (
            context.metadata.get("partition_by") if context.metadata else None
        )

        writer = obj.write.mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.parquet(path)
        context.log.info(f"Saved Spark DataFrame for asset {asset_name} to {path}")
        return AssetMaterialization(
            asset_key=context.asset_key,
            description=f"Saved Spark DataFrame for {asset_name}",
            metadata={"path": MetadataValue.path(path)},
        )

    def load_input(self, context):
        asset_name = (
            context.upstream_output.asset_key.path[-1]
            if context.upstream_output.asset_key
            else context.upstream_output.step_key
        )

        path = _get_path(self, asset_name, context)
        df = self.spark.read.parquet(path)
        context.log.info(f"Loaded Spark DataFrame from {path}")
        return df


def _get_path(self, asset_name, context):
    if context.metadata and "relative_path" in context.metadata:
        return os.path.join(
            self.base_path, context.metadata.get("relative_path")[0], asset_name
        )
    else:
        return os.path.join(self.base_path, asset_name)


@io_manager(required_resource_keys={"spark_session_resource"})
def spark_parquet_io_manager(init_context):
    spark = init_context.resources.spark_session_resource
    base_path = init_context.resource_config.get("base_path")
    return SparkParquetIOManager(base_path=base_path, spark=spark)
