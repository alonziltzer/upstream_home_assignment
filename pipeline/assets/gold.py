from pyspark.sql import functions as F
from pyspark.sql.window import Window
from dagster import asset
from pyspark.sql import DataFrame


@asset(name="Gold_vin_last_state_report")
def gold_vin_last_state_report_asset(Silver: DataFrame):
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


@asset(name="gold_top_10_fastest_vehicles_per_date_hour_report")
def gold_top_10_fastest_vehicles_per_date_hour_report_asset(Silver: DataFrame):
    return _top_k_fastest_vehicles_per_hour_report(Silver, k=10)


def _top_k_fastest_vehicles_per_hour_report(silver, k):
    df = silver.withColumn("date_hour", F.concat_ws("-", F.col("date"), F.col("hour")))

    vehicle_max_velocity = df.groupBy("vin", "date_hour").agg(
        F.max("velocity").alias("top_velocity")
    )

    w = Window.partitionBy("date_hour").orderBy(F.col("top_velocity").desc())

    return (
        vehicle_max_velocity.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= k)
        .drop("rank")
        .orderBy("date_hour", F.col("top_velocity").desc())
    )
