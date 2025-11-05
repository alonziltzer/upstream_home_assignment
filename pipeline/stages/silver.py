from pyspark.sql import functions as F
import os
from pipeline.stages.common import create_spark_session, get_silver_path, get_bronze_path


def silver_stage():
    print("Silver stage Started")
    spark = create_spark_session("silver")
    df = _read_input(spark)
    df = _remove_trailing_spaces_manufacturer(df)
    df = _remove_null_vin(df)
    df = _standerd_gear_positions_to_integers(df)
    _write_output(df)
    print("Silver stage completed")


def _standerd_gear_positions_to_integers(df):
    print("standerd_gear_positions_to_intergers stage started")
    df.groupby("gearPosition").count().show(100)
    df = df_clean = (
        df.withColumn(
            "gearPosition",
            F.when(F.col("gearPosition") == "REVERSE", F.lit(-1))
            .when(F.col("gearPosition") == "NEUTRAL", F.lit(0))
            .otherwise(F.col("gearPosition").cast("int"))
        ).filter(F.col("gearPosition").isNotNull())  # remove NULLs
    )
    df.groupby("gearPosition").count().show(100)
    return df


def _read_input(spark):
    bronze_path = get_bronze_path()
    return spark.read.parquet(bronze_path)


def _write_output(df):
    silver_path = get_silver_path()
    df.write.mode("overwrite").parquet(silver_path)


def _remove_trailing_spaces_manufacturer(df):
    print(df.count())
    df.groupby("manufacturer").count().show(100)
    df = df.filter(~F.col("manufacturer").rlike(r"\s+$"))
    print(df.count())
    df.groupby("manufacturer").count().show(100)
    return df


def _remove_null_vin(df):
    print("before vin filter count", df.count())
    df.filter(~F.col("vin").isNotNull()).show(5)
    df.filter(F.col("vin").isNotNull()).show(5)
    df = df.filter(F.col("vin").isNotNull())
    print("after vin filter count", df.count())
    return df


if __name__ == "__main__":
    silver()
