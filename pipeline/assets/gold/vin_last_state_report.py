from pyspark.sql import functions as F
from dagster import asset
from pyspark.sql import DataFrame


@asset(name="vin_last_state_report", metadata={"relative_path": ["Gold"]})
def vin_last_state_report_asset(Silver: DataFrame):
    return Silver.groupBy("vin").agg(
        F.max("timestamp").alias("last_reported_timestamp"),
        F.max(
            F.when(
                F.col("frontLeftDoorState").isNotNull(),
                F.col("frontLeftDoorState"),
            )
        ).alias("front_left_door_state"),
        F.max(F.when(F.col("wipersState").isNotNull(), F.col("wipersState"))).alias(
            "wipers_state"
        ),
    )
