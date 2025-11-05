import pandas as pd
from pipeline.assets.gold import _top_k_fastest_vehicles_per_hour_report
from pipeline.dagster.resources import spark_session_resource


def test_top_10_fastest_vehicles_per_date_hour_report():
    data = [
        {"vin": "VIN1", "velocity": 50, "date": "2023-11-15", "hour": "00"},
        {"vin": "VIN2", "velocity": 60, "date": "2023-11-15", "hour": "00"},
        {"vin": "VIN1", "velocity": 70, "date": "2023-11-15", "hour": "01"},
        {"vin": "VIN2", "velocity": 65, "date": "2023-11-15", "hour": "01"},
        {"vin": "VIN3", "velocity": 40, "date": "2023-11-15", "hour": "01"},
    ]
    with spark_session_resource() as spark:
        silver = spark.createDataFrame(data)

        actual = _top_k_fastest_vehicles_per_hour_report(silver, k=2)

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
