from dagster import resource
from pyspark.sql import SparkSession


@resource()
def spark_session_resource():
    spark = (
        SparkSession.builder.appName("upstream_assignment")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()
