from dagster import materialize, asset

from pipeline.assets.bonus import bonus_asset
from pipeline.config import DATA_LAKE_PATH
from pipeline.dagster.resources import spark_session_resource
from pipeline.dagster.spark_io_manager import spark_parquet_io_manager
from pandas.testing import assert_frame_equal
import pandas as pd


@asset(name="Bronze")
def bronze_source():
    with spark_session_resource() as spark:
        data = [
            {"vin1": "123ABC", "vin2": "123ABC"},
            {"vin1": "456'; DROP TABLE users;--", "vin2": "123ABC"},
            {"vin1": "789XYZ", "vin2": "SELECT * FROM sensitive_table"},
            {
                "vin1": "SELECT * FROM sensitive_table2",
                "vin2": "SELECT * FROM sensitive_table3",
            },
        ]
        return spark.createDataFrame(data)


def test_sql_injection_report():
    actual = materialize(
        [bronze_source, bonus_asset],
        resources={
            "spark_session_resource": spark_session_resource,
            "io_manager": spark_parquet_io_manager.configured(
                {"base_path": DATA_LAKE_PATH}
            ),
        },
        run_config={
            "ops": {
                "sql_injection_report": {
                    "config": {
                        "columns": ["vin1", "vin2"],
                        "regex_list": [".*TABLE.*", ".*FROM.*"],
                    }
                }
            }
        },
    )

    bonus_event = next(
        e
        for e in actual.get_asset_materialization_events()
        if e.asset_key.path[-1] == "sql_injection_report"
    )

    path = bonus_event.event_specific_data.materialization.metadata["path"].value

    with spark_session_resource() as spark:
        actual = spark.read.parquet(path).sort("original_column", "violating_message")
        actual.show(truncate=False)

        expected_pdf = pd.DataFrame(
            [
                {
                    "original_column": "vin1",
                    "violating_message": "456'; DROP TABLE users;--",
                    "pattern_matched": ".*TABLE.*",
                },
                {
                    "original_column": "vin1",
                    "violating_message": "SELECT * FROM sensitive_table2",
                    "pattern_matched": ".*FROM.*",
                },
                {
                    "original_column": "vin2",
                    "violating_message": "SELECT * FROM sensitive_table",
                    "pattern_matched": ".*FROM.*",
                },
                {
                    "original_column": "vin2",
                    "violating_message": "SELECT * FROM sensitive_table3",
                    "pattern_matched": ".*FROM.*",
                },
            ]
        )

        # Use pandas testing assert
        assert_frame_equal(actual.toPandas(), expected_pdf)
