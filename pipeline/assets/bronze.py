import requests
from pyspark.sql import functions as F

from dagster import asset, OpExecutionContext
from pyspark.sql import DataFrame


@asset(
    name="Bronze",
    required_resource_keys={"spark_session_resource", "io_manager"},
    metadata={"partition_by": ["date", "hour"]},
)
def bronze_asset(context: OpExecutionContext) -> DataFrame:
    data = _get_data_from_service()
    df = _transform_to_spark_df(context, data)
    df = _add_date_and_hour_from_timestamp(df)
    return df


def _transform_to_spark_df(context, data):
    spark = context.resources.spark_session_resource
    return spark.createDataFrame(data)


def _add_date_and_hour_from_timestamp(df):
    df = df.withColumn("timestamp_dt", (df.timestamp / 1000).cast("timestamp"))
    df = df.withColumn("date", F.date_format("timestamp_dt", "yyyy-MM-dd"))
    return df.withColumn("hour", F.date_format("timestamp_dt", "HH"))


def _get_data_from_service(amount: int = 10000):
    try:
        response = requests.get(
            f"http://localhost:9900/upstream/vehicle_messages?amount={amount}"
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        raise (f"Error fetching data from service: {e}")
