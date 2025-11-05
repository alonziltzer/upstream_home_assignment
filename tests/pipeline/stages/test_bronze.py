import pytest
from unittest.mock import patch
from pipeline.stages.bronze import bronze_stage, _get_data_from_service
import os

from pipeline.stages.common import create_spark_session


# Sample data


@patch("pipeline.stages.bronze._get_data_from_service")
def test_bronze_stage(mock_get_data):
    MOCK_DATA = [
        {
            "vin": "VIN1",
            "manufacturer": "Toyota",
            "timestamp": 1700000000000,
        },  # 2023-11-14 / hour 20
        {
            "vin": "VIN2",
            "manufacturer": "Honda",
            "timestamp": 1700000000000,
        },  # 2023-11-14 / hour 20
        {
            "vin": "VIN3",
            "manufacturer": "Ford",
            "timestamp": 1700003600000,
        },  # 2023-11-14 / hour 21
        {
            "vin": "VIN4",
            "manufacturer": "Chevy",
            "timestamp": 1700003600000,
        },  # 2023-11-14 / hour 21
        {
            "vin": "VIN5",
            "manufacturer": "Nissan",
            "timestamp": 1700086400000,
        },  # 2023-11-15 / hour 20
        {
            "vin": "VIN6",
            "manufacturer": "Mazda",
            "timestamp": 1700086400000,
        },  # 2023-11-15 / hour 20
        {
            "vin": "VIN7",
            "manufacturer": "Kia",
            "timestamp": 1700090000000,
        },  # 2023-11-15 / hour 21
        {
            "vin": "VIN8",
            "manufacturer": "Subaru",
            "timestamp": 1700090000000,
        },  # 2023-11-15 / hour 21
    ]

    mock_get_data.return_value = MOCK_DATA

    bronze_path = bronze_stage()

    assert os.path.exists(bronze_path)
    spark = create_spark_session("test_bronze")
    df = spark.read.parquet(bronze_path)
    df_count = df.count()
    assert df_count == 8
    _check_partition(spark, bronze_path + "/date=2023-11-15/hour=00", {"VIN1", "VIN2"})
    _check_partition(spark, bronze_path + "/date=2023-11-15/hour=01", {"VIN3", "VIN4"})
    _check_partition(spark, bronze_path + "/date=2023-11-16/hour=00", {"VIN5", "VIN6"})
    _check_partition(spark, bronze_path + "/date=2023-11-16/hour=01", {"VIN7", "VIN8"})


@patch("pipeline.stages.bronze.requests.get")
def test_get_data_raises_exception(mock_get):
    mock_get.side_effect = Exception("Service unavailable")

    with pytest.raises(Exception) as e:
        _get_data_from_service()

    assert "Service unavailable" in str(e.value)


def _check_partition(spark, path, expected_vin):
    assert os.path.exists(path)
    df = spark.read.parquet(path)
    actual_vin = {row.vin for row in df.select("vin").distinct().collect()}
    assert expected_vin == actual_vin
