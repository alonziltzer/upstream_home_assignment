from pyspark.sql import functions as F
from dagster import asset
from pyspark.sql import DataFrame


@asset(name="Silver", required_resource_keys={"spark_session_resource"})
def silver_asset(Bronze: DataFrame):
    bronze = _fix_trailing_spaces_in_manufacturer(Bronze)
    bronze = _remove_null_vin(bronze)
    return _standard_gear_positions_to_integers(bronze)


def _standard_gear_positions_to_integers(df):
    return df.withColumn(
        "gearPosition",
        F.when(F.col("gearPosition") == "REVERSE", F.lit(-1))
        .when(F.col("gearPosition") == "NEUTRAL", F.lit(0))
        .otherwise(F.col("gearPosition").cast("int")),
    ).filter(F.col("gearPosition").isNotNull())


def _fix_trailing_spaces_in_manufacturer(df):
    return df.withColumn("manufacturer", F.rtrim(F.col("manufacturer")))


def _remove_null_vin(df):
    return df.filter(F.col("vin").isNotNull())
