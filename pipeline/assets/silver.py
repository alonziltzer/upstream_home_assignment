from pyspark.sql import functions as F
from dagster import asset
from pyspark.sql import DataFrame


@asset(name="Silver", required_resource_keys={"spark_session_resource"})
def silver_asset(Bronze: DataFrame):
    bronze = _fix_trailing_spaces_in_manufacturer(Bronze)
    bronze = _remove_null_vin(bronze)
    return _standard_gear_positions_to_integers(bronze)


def _standard_gear_positions_to_integers(df):
    valid_gear_positions = ["-1", "0", "1", "2", "3", "4", "5", "6"]
    return df.withColumn(
        "gearPosition",
        F.when(F.col("gearPosition") == "REVERSE", F.lit(-1))
        .when(F.col("gearPosition") == "NEUTRAL", F.lit(0))
        .when(
            F.col("gearPosition").isin(valid_gear_positions),
            F.col("gearPosition").cast("int"),
        )
        .when(F.col("gearPosition").isNull(), F.lit(-1000))
        .otherwise(F.lit(-1001)),
    )


def _fix_trailing_spaces_in_manufacturer(df):
    return df.withColumn("manufacturer", F.rtrim(F.col("manufacturer")))


def _remove_null_vin(df):
    return df.filter(F.col("vin").isNotNull())
