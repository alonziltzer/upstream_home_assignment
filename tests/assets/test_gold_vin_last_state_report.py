from pipeline.assets.gold.vin_last_state_report import vin_last_state_report_asset
from pandas.testing import assert_frame_equal
import pandas as pd
from pipeline.dagster.resources import spark_session_resource


def test_vin_last_state_report():
    with spark_session_resource() as spark:
        silver = _create_vin_last_state_report_input(spark)
        actual = vin_last_state_report_asset(silver).orderBy("vin")
        expected = [
            {
                "vin": "VIN1",
                "last_reported_timestamp": 1700003600000,
                "front_left_door_state": "OPEN",
                "wipers_state": "ON",
            },
            {
                "vin": "VIN2",
                "last_reported_timestamp": 1700003600000,
                "front_left_door_state": "CLOSED",
                "wipers_state": "OFF",
            },
            {
                "vin": "VIN3",
                "last_reported_timestamp": 1700003600000,
                "front_left_door_state": None,
                "wipers_state": None,
            },
        ]

        expected = pd.DataFrame(expected)

        assert_frame_equal(actual.toPandas(), expected)


def _create_vin_last_state_report_input(spark):
    input = [
        # all data exists in last record
        {
            "vin": "VIN1",
            "timestamp": 1700000000000,
            "frontLeftDoorState": None,
            "wipersState": None,
        },
        {
            "vin": "VIN1",
            "timestamp": 1700003600000,
            "frontLeftDoorState": "OPEN",
            "wipersState": "ON",
        },
        # none data
        {
            "vin": "VIN2",
            "timestamp": 1700000000000,
            "frontLeftDoorState": "CLOSED",
            "wipersState": "OFF",
        },
        {
            "vin": "VIN2",
            "timestamp": 1700003600000,
            "frontLeftDoorState": None,
            "wipersState": None,
        },
        # all none
        {
            "vin": "VIN3",
            "timestamp": 1700000000000,
            "frontLeftDoorState": None,
            "wipersState": None,
        },
        {
            "vin": "VIN3",
            "timestamp": 1700003600000,
            "frontLeftDoorState": None,
            "wipersState": None,
        },
    ]
    return spark.createDataFrame(input)
