from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipeline.stages.common import get_silver_path, get_gold_path, create_spark_session
import logging


def gold_stage():
    logging.info("gold stage started")
    #_vin_last_state_report()
    # generate_top_10_fastest_vehicles()
    logging.info("gold stage finished")


def _vin_last_state_report():
    logging.info("start vin last state report")
    spark = create_spark_session("gold_stage")
    silver_path = get_silver_path()
    gold_path = get_gold_path()

    df = spark.read.parquet(silver_path)
    window = Window.partitionBy("vin").orderBy(F.col("timestamp").desc())
    df = df.withColumn("rn", F.row_number().over(window))
    last_state = df.filter(F.col("rn") == 1).select(
        F.col("vin"),
        F.col("timestamp").alias("last_reported_timestamp"),
        F.col("frontLeftDoorState").alias("front_left_door_state"),
        F.col("wipersState").alias("wipers_state")
    )
    report_path = os.path.join(gold_path, "vin_last_state")
    last_state.write.mode("overwrite").parquet()
    print("Gold vin_last_state report completed")


def generate_top_10_fastest_vehicles():
    spark = SparkSession.builder.appName("GoldTopFastest").getOrCreate()
    silver_path = get_silver_path()
    df = spark.read.parquet(silver_path)

    df = df.withColumn("datetime", F.from_unixtime(F.col("timestamp") / 1000)) \
        .withColumn("date", F.to_date(F.col("datetime"))) \
        .withColumn("hour", F.hour(F.col("datetime")))

    window = Window.partitionBy("date", "hour").orderBy(F.col("velocity").desc())
    top_df = df.withColumn("rank", F.row_number().over(window)) \
        .filter(F.col("rank") <= 10) \
        .select("vin", "date", "hour", "velocity")

    gold_path = get_gold_path()
    top_df.write.mode("overwrite").parquet(gold_path)


def gold_top_fastest_op(data_lake_path: str = "data_lake"):
    spark = SparkSession.builder.appName("gold_top_fastest").getOrCreate()
    silver_path = os.path.join(data_lake_path, "silver")
    gold_path = os.path.join(data_lake_path, "gold")
    os.makedirs(gold_path, exist_ok=True)

    df = spark.read.parquet(silver_path)
    df = df.withColumn("date_hour", F.date_format(F.col("timestamp") / 1000, "yyyy-MM-dd-HH"))

    window = Window.partitionBy("date_hour").orderBy(F.col("velocity").desc())
    df = df.withColumn("rank", F.row_number().over(window))
    top_vehicles = df.filter(F.col("rank") <= 10).select("vin", "date_hour", "velocity")

    top_vehicles.write.mode("overwrite").parquet(os.path.join(gold_path, "top_10_fastest"))
    print("✅ Gold top 10 fastest report completed")


if __name__ == "__main__":
    gold()
