from pyspark.sql import functions as F
from pipeline.stages.common import create_spark_session, get_silver_path, get_bronze_path


def silver_stage():
    spark = create_spark_session("silver")
    df = _read_input(spark)
    df = _fix_trailing_spaces_in_manufacturer(df)
    df = _remove_null_vin(df)
    df = _standard_gear_positions_to_integers(df)
    _write_output(df)


def _standard_gear_positions_to_integers(df):
    return df.withColumn(
        "gearPosition",
        F.when(F.col("gearPosition") == "REVERSE", F.lit(-1))
        .when(F.col("gearPosition") == "NEUTRAL", F.lit(0))
        .otherwise(F.col("gearPosition").cast("int"))
    ).filter(F.col("gearPosition").isNotNull())


def _read_input(spark):
    bronze_path = get_bronze_path()
    return spark.read.parquet(bronze_path)


def _write_output(df):
    silver_path = get_silver_path()
    df.write.mode("overwrite").parquet(silver_path)


def _fix_trailing_spaces_in_manufacturer(df):
    return df.withColumn("manufacturer", F.rtrim(F.col("manufacturer")))


def _remove_null_vin(df):
    return df.filter(F.col("vin").isNotNull())
