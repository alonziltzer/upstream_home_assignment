from pyspark.sql import SparkSession
import os
import requests
from pyspark.sql.functions import date_format

from pipeline.stages.common import get_bronze_path, create_spark_session


#
def bronze_stage():
    print("Bronze stage started")
    data = _get_data_from_service()
    df = _transform_to_spark_df(data)
    df = _add_date_and_hour_from_timestamp(df)
    _write_parquet_to_bronze_path(df)
    print("Bronze stage completed")


def _transform_to_spark_df(data):
    spark = create_spark_session("bronze")
    return spark.createDataFrame(data)


def _add_date_and_hour_from_timestamp(df):
    df = df.withColumn("timestamp_dt", (df.timestamp / 1000).cast("timestamp"))
    df = df.withColumn("date", date_format("timestamp_dt", "yyyy-MM-dd"))
    df = df.withColumn("hour", date_format("timestamp_dt", "HH"))
    df.show(5)
    return df


def _write_parquet_to_bronze_path(df):
    bronze_path = get_bronze_path()
    df.write.mode("overwrite").partitionBy("date", "hour").parquet(bronze_path)


def _get_data_from_service(amount: int = 10000):
    return requests.get(f"http://localhost:9900/upstream/vehicle_messages?amount={amount}").json()


if __name__ == "__main__":
    bronze_stage()
