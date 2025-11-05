from pyspark.sql import SparkSession
import os


def create_spark_session(stage_name):
    return SparkSession.builder \
        .appName(stage_name) \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()


def _get_data_lake_path():
    return "/Users/aziltzer/projects/upstream_home_assignment/data_lake"


def get_bronze_path():
    data_lake_path = _get_data_lake_path()
    return os.path.join(data_lake_path, "Bronze")


def get_silver_path():
    data_lake_path = _get_data_lake_path()
    return os.path.join(data_lake_path, "Silver")


def get_gold_path():
    data_lake_path = _get_data_lake_path()
    return os.path.join(data_lake_path, "Gold")
