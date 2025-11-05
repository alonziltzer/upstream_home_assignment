from pipeline.stages.common import create_spark_session
from pipeline.stages.silver import (
    _fix_trailing_spaces_in_manufacturer,
    _remove_null_vin,
    _standard_gear_positions_to_integers,
)


def test_remove_trailing_spaces_manufacturer():
    data = [
        {"vin": "VIN1", "manufacturer": "Toyota "},
        {"vin": "VIN2", "manufacturer": "Honda"},
        {"vin": "VIN3", "manufacturer": "Ford    "},
        {"vin": "VIN3", "manufacturer": " Fiat"},
    ]
    df = create_spark_session("test_silver").createDataFrame(data)
    actual = _fix_trailing_spaces_in_manufacturer(df)
    assert actual.count() == 4
    assert {row.manufacturer for row in actual.select("manufacturer").collect()} == {
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
    df = create_spark_session("test_silver").createDataFrame(data)
    actual = _remove_null_vin(df)
    assert actual.count() == 1
    assert actual.first().vin == "VIN4"


def test_standard_gear_positions_to_integers():
    data = [
        {"vin": "VIN1", "manufacturer": "Toyota", "gearPosition": "REVERSE"},
        {"vin": "VIN2", "manufacturer": "Honda", "gearPosition": "NEUTRAL"},
        {"vin": "VIN2", "manufacturer": "Honda", "gearPosition": "None"},
        {"vin": "VIN3", "manufacturer": "Ford", "gearPosition": "1"},
        {"vin": "VIN4", "manufacturer": "Chevy", "gearPosition": "5"},
    ]
    df = create_spark_session("test_silver").createDataFrame(data)
    df_clean = _standard_gear_positions_to_integers(df)
    gear_values = set(
        [row.gearPosition for row in df_clean.select("gearPosition").collect()]
    )
    expected_values = {-1, 0, 1, 5}
    assert gear_values == expected_values
