import pipeline.stages.gold as gold
from pipeline.stages.common import get_gold_path, create_spark_session
import os
from pandas.testing import assert_frame_equal
import pandas as pd
from pipeline.stages.gold import _top_k_fastest_vehicles_per_hour_report


def test_vin_last_state_report():
    spark = create_spark_session("test_gold")
    silver = _create_vin_last_state_report_input(spark)
    gold._vin_last_state_report(silver)
    actual = spark.read.parquet(
        os.path.join(get_gold_path(), "vin_last_state_report")
    ).orderBy("vin")
    expected = [
        {
            "vin": "VIN1",
            "last_reported_timestamp": 1700003600000,
            "front_left_door_state": "OPEN",
            "last_wipers_state": "ON",
        },
        {
            "vin": "VIN2",
            "last_reported_timestamp": 1700003600000,
            "front_left_door_state": "CLOSED",
            "last_wipers_state": "OFF",
        },
        {
            "vin": "VIN3",
            "last_reported_timestamp": 1700003600000,
            "front_left_door_state": None,
            "last_wipers_state": None,
        },
    ]

    expected = pd.DataFrame(expected)

    assert_frame_equal(actual.toPandas(), expected)


def test_top_10_fastest_vehicles_per_date_hour_report():
    spark = create_spark_session("test_gold")
    data = [
        {"vin": "VIN1", "velocity": 50, "date": "2023-11-15", "hour": "00"},
        {"vin": "VIN2", "velocity": 60, "date": "2023-11-15", "hour": "00"},
        {"vin": "VIN1", "velocity": 70, "date": "2023-11-15", "hour": "01"},
        {"vin": "VIN2", "velocity": 65, "date": "2023-11-15", "hour": "01"},
        {"vin": "VIN3", "velocity": 40, "date": "2023-11-15", "hour": "01"},
    ]
    silver = spark.createDataFrame(data)

    _top_k_fastest_vehicles_per_hour_report(silver, k=2)
    actual = spark.read.parquet(
        os.path.join(get_gold_path(), "top_10_fastest_vehicles_per_date_hour_report")
    )
    actual.show()

    expected = pd.DataFrame(
        {
            "vin": ["VIN2", "VIN1", "VIN1", "VIN2"],
            "date_hour": [
                "2023-11-15-00",
                "2023-11-15-00",
                "2023-11-15-01",
                "2023-11-15-01",
            ],
            "top_velocity": [60, 50, 70, 65],
        }
    )

    pd.testing.assert_frame_equal(actual.toPandas(), expected)


def _create_vin_last_state_report_input(spark):
    input = [
        # all data exists in last record
        {
            "vin": "VIN1",
            "timestamp": 1700000000000,
            "front_left_door_state": None,
            "wipers_state": None,
        },
        {
            "vin": "VIN1",
            "timestamp": 1700003600000,
            "front_left_door_state": "OPEN",
            "wipers_state": "ON",
        },
        # none data
        {
            "vin": "VIN2",
            "timestamp": 1700000000000,
            "front_left_door_state": "CLOSED",
            "wipers_state": "OFF",
        },
        {
            "vin": "VIN2",
            "timestamp": 1700003600000,
            "front_left_door_state": None,
            "wipers_state": None,
        },
        # all none
        {
            "vin": "VIN3",
            "timestamp": 1700000000000,
            "front_left_door_state": None,
            "wipers_state": None,
        },
        {
            "vin": "VIN3",
            "timestamp": 1700003600000,
            "front_left_door_state": None,
            "wipers_state": None,
        },
    ]
    return spark.createDataFrame(input)
