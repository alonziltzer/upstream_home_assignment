import requests
from pyspark.sql.functions import date_format

from pipeline.stages.common import get_bronze_path, create_spark_session


def bronze_stage():
    data = _get_data_from_service()
    df = _transform_to_spark_df(data)
    df = _add_date_and_hour_from_timestamp(df)
    path = _write_parquet_to_bronze_path_partition_by_date_and_hour(df)
    return path


def _transform_to_spark_df(data):
    spark = create_spark_session("bronze")
    return spark.createDataFrame(data)


def _add_date_and_hour_from_timestamp(df):
    df = df.withColumn("timestamp_dt", (df.timestamp / 1000).cast("timestamp"))
    df = df.withColumn("date", date_format("timestamp_dt", "yyyy-MM-dd"))
    df = df.withColumn("hour", date_format("timestamp_dt", "HH"))
    return df


def _write_parquet_to_bronze_path_partition_by_date_and_hour(df):
    bronze_path = get_bronze_path()
    df.write.mode("overwrite").partitionBy("date", "hour").parquet(bronze_path)
    return bronze_path


def _get_data_from_service(amount: int = 10000):
    try:
        response = requests.get(
            f"http://localhost:9900/upstream/vehicle_messages?amount={amount}"
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        raise (f"Error fetching data from service: {e}")
