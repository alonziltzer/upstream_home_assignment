import os
from pyspark.sql import functions as F
from pipeline.stages.common import get_silver_path, create_spark_session
from pipeline.stages.common import get_gold_path
from pyspark.sql.window import Window


def gold_stage():
    silver = create_spark_session("gold").read.parquet(get_silver_path())
    _vin_last_state_report(silver)
    _top_k_fastest_vehicles_per_hour_report(silver, k=10)


def _vin_last_state_report(silver):
    last_state = silver.groupBy("vin").agg(
        F.max("timestamp").alias("last_reported_timestamp"),
        F.max(
            F.when(
                F.col("front_left_door_state").isNotNull(),
                F.col("front_left_door_state"),
            )
        ).alias("front_left_door_state"),
        F.max(F.when(F.col("wipers_state").isNotNull(), F.col("wipers_state"))).alias(
            "last_wipers_state"
        ),
    )

    report_path = os.path.join(get_gold_path(), "vin_last_state_report")
    last_state.write.mode("overwrite").parquet(report_path)


def _top_k_fastest_vehicles_per_hour_report(silver, k):
    df = silver.withColumn("date_hour", F.concat_ws("-", F.col("date"), F.col("hour")))

    vehicle_max_velocity = df.groupBy("vin", "date_hour").agg(
        F.max("velocity").alias("top_velocity")
    )

    w = Window.partitionBy("date_hour").orderBy(F.col("top_velocity").desc())

    top_vehicles = (
        vehicle_max_velocity.withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= k)
        .drop("rank")
        .orderBy("date_hour", F.col("top_velocity").desc())
    )

    report_path = os.path.join(
        get_gold_path(), "top_10_fastest_vehicles_per_date_hour_report"
    )

    top_vehicles.write.mode("overwrite").parquet(report_path)
