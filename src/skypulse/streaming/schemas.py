"""Spark schemas derived from the JSON Schema contracts.

The contracts in ``contracts/`` stay the single source of truth: silver tables never declare
their own column types. Only the subset of JSON Schema the contracts use is supported, and
anything else fails loudly instead of silently becoming a string.
"""

from collections.abc import Mapping
from typing import Any

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from skypulse.contracts import EventType, contract_for

_SCALARS: Mapping[str, DataType] = {
    "string": StringType(),
    "integer": LongType(),
    "number": DoubleType(),
    "boolean": BooleanType(),
}
_PYTHON_TYPES: Mapping[type, str] = {
    str: "string",
    int: "integer",
    float: "number",
    bool: "boolean",
}


def spark_type(schema: Mapping[str, Any]) -> tuple[DataType, bool]:
    """Spark type of a JSON Schema node and whether it admits null."""
    if "const" in schema:
        return _from_values([schema["const"]])
    if "enum" in schema:
        return _from_values(schema["enum"])
    declared = schema.get("type")
    names = declared if isinstance(declared, list) else [declared]
    nullable = "null" in names
    concrete = [str(name) for name in names if name not in ("null", None)]
    if concrete == ["object"]:
        return _struct(schema), nullable
    if concrete == ["array"]:
        item_type, item_nullable = spark_type(schema["items"])
        return ArrayType(item_type, item_nullable), nullable
    if len(concrete) != 1 or concrete[0] not in _SCALARS:
        raise ValueError(f"unsupported JSON Schema node: {dict(schema)}")
    if concrete[0] == "string" and schema.get("format") == "date-time":
        return TimestampType(), nullable
    return _SCALARS[concrete[0]], nullable


def _from_values(values: list[Any]) -> tuple[DataType, bool]:
    kinds = {_PYTHON_TYPES[type(value)] for value in values if value is not None}
    if len(kinds) != 1:
        raise ValueError(f"unsupported mixed enum: {values}")
    return _SCALARS[kinds.pop()], None in values


def _struct(schema: Mapping[str, Any]) -> StructType:
    fields = []
    for name, node in schema.get("properties", {}).items():
        data_type, nullable = spark_type(node)
        fields.append(StructField(name, data_type, nullable))
    return StructType(fields)


def _relaxed(data_type: DataType) -> DataType:
    if not isinstance(data_type, StructType):
        return data_type
    return StructType([StructField(f.name, _relaxed(f.dataType), True) for f in data_type.fields])


def event_schema(event_type: EventType) -> StructType:
    """Read schema for ``from_json``: everything nullable, required fields checked afterwards."""
    data_type, _ = spark_type(contract_for(event_type).schema)
    relaxed = _relaxed(data_type)
    if not isinstance(relaxed, StructType):
        raise TypeError(f"contract for {event_type} is not an object")
    return relaxed


def required_data_fields(event_type: EventType) -> list[str]:
    """Payload fields that the contract requires and does not allow to be null."""
    data_schema = contract_for(event_type).schema["properties"]["data"]
    return [
        name
        for name in data_schema["required"]
        if not spark_type(data_schema["properties"][name])[1]
    ]
