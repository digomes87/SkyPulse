import pytest
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from skypulse.contracts import EventType, contract_for
from skypulse.streaming.schemas import event_schema, required_data_fields, spark_type


def test_maps_scalar_types() -> None:
    assert spark_type({"type": "string"}) == (StringType(), False)
    assert spark_type({"type": "integer"}) == (LongType(), False)
    assert spark_type({"type": "number"}) == (DoubleType(), False)
    assert spark_type({"type": "boolean"}) == (BooleanType(), False)


def test_date_time_strings_become_timestamps() -> None:
    assert spark_type({"type": "string", "format": "date-time"}) == (TimestampType(), False)


def test_nullable_unions_are_nullable() -> None:
    assert spark_type({"type": ["number", "null"]}) == (DoubleType(), True)


def test_enums_and_consts_take_the_type_of_their_values() -> None:
    assert spark_type({"enum": [0, 1, 2]}) == (LongType(), False)
    assert spark_type({"const": "metar_observation"}) == (StringType(), False)
    assert spark_type({"enum": ["VFR", None]}) == (StringType(), True)


def test_objects_become_structs_in_declared_order() -> None:
    schema = {
        "type": "object",
        "properties": {"b": {"type": "string"}, "a": {"type": ["integer", "null"]}},
    }

    assert spark_type(schema) == (
        StructType([StructField("b", StringType(), False), StructField("a", LongType(), True)]),
        False,
    )


def test_arrays_become_array_types() -> None:
    assert spark_type({"type": "array", "items": {"type": "integer"}}) == (
        ArrayType(LongType(), False),
        False,
    )


def test_unsupported_schemas_fail_loudly() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        spark_type({"type": ["string", "integer"]})


@pytest.mark.parametrize("event_type", list(EventType))
def test_every_contract_converts_to_a_nullable_read_schema(event_type: EventType) -> None:
    schema = event_schema(event_type)

    assert schema.fieldNames() == [
        "event_id", "event_type", "schema_version", "source", "produced_at", "data"
    ]  # fmt: skip
    assert all(field.nullable for field in schema.fields)


def test_required_data_fields_are_the_non_nullable_required_ones() -> None:
    required = required_data_fields(EventType.METAR_OBSERVATION)

    assert "station_icao" in required
    assert "wind_gust_kt" not in required
    assert set(required) <= set(
        contract_for(EventType.METAR_OBSERVATION).schema["properties"]["data"]["required"]
    )
