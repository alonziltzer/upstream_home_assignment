from pipeline.assets.silver import (
    _fix_trailing_spaces_in_manufacturer,
    _remove_null_vin,
    _standard_gear_positions_to_integers,
)
from pipeline.dagster.resources import spark_session_resource


def test_remove_trailing_spaces_manufacturer():
    data = [
        {"vin": "VIN1", "manufacturer": "Toyota "},
        {"vin": "VIN2", "manufacturer": "Honda"},
        {"vin": "VIN3", "manufacturer": "Ford    "},
        {"vin": "VIN3", "manufacturer": " Fiat"},
    ]
    with spark_session_resource() as spark:
        df = spark.createDataFrame(data)
        actual = _fix_trailing_spaces_in_manufacturer(df)
        assert actual.count() == 4
        assert {
            row.manufacturer for row in actual.select("manufacturer").collect()
        } == {
            "Toyota",
            "Honda",
            "Ford",
            " Fiat",
        }


def test_remove_null_vin():
    data = [
        {"vin": None, "manufacturer": "Ford", "gearPosition": "1"},
        {"vin": "VIN4", "manufacturer": "Chevy", "gearPosition": "5"},
    ]
    with spark_session_resource() as spark:
        df = spark.createDataFrame(data)
        actual = _remove_null_vin(df)
        assert actual.count() == 1
        assert actual.first().vin == "VIN4"


def test_standard_gear_positions_to_integers():
    data = [
        {"vin": "VIN1", "manufacturer": "Toyota", "gearPosition": "REVERSE"},
        {"vin": "VIN2", "manufacturer": "Honda", "gearPosition": "NEUTRAL"},
        {"vin": "VIN3", "manufacturer": "Honda", "gearPosition": None},
        {"vin": "VIN4", "manufacturer": "Ford", "gearPosition": "1"},
        {"vin": "VIN5", "manufacturer": "Chevy", "gearPosition": "5"},
        {"vin": "VIN6", "manufacturer": "Seat", "gearPosition": "asdad"},
    ]
    with spark_session_resource() as spark:
        df = spark.createDataFrame(data)

        actual = _standard_gear_positions_to_integers(df)
        actual = {
            row.vin: row.gearPosition
            for row in actual.select("vin", "gearPosition").collect()
        }

        expected_values = {
            "VIN1": -1,
            "VIN2": 0,
            "VIN3": -1000,
            "VIN4": 1,
            "VIN5": 5,
            "VIN6": -1001,
        }

        assert actual == expected_values
