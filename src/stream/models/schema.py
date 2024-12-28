import json
from datetime import datetime, date, time, timedelta
from decimal import Decimal, DecimalException, getcontext, ROUND_HALF_EVEN
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Type,
    Union,
    get_args,
    get_origin,
    Literal,
    TypeVar,
    Generic,
)
from typing import Any, Dict, Type
from enum import Enum

import attrs

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

##############################################################################
#               Python -> Avro -> Flink  Type Mappings (High-Level)          #
##############################################################################
# - All datetime fields (Python's datetime) map to:
#       Avro:  {"type": "long",  "logicalType": "local-timestamp-millis"}
#       Flink: TIMESTAMP(3)
#       Internally: # of milliseconds since the Unix epoch
#
# - All time (Python's time) map to:
#       Avro:  {"type": "int",   "logicalType": "time-millis"}
#       Flink: TIME (with up to 3 digits of precision)
#       Internally: # of milliseconds since midnight
#
# - All date (Python's date) map to:
#       Avro:  {"type": "int",   "logicalType": "date"}
#       Flink: DATE
#       Internally: # of days since the Unix epoch
#
# - Decimal -> Avro (bytes + decimal) -> Flink DECIMAL
#
# - Enums in Flink are typically treated as STRING in practice,
#   but you can also produce Avro "enum" for your own environment.
##############################################################################

#
# Avro <-> Flink baseline from your mapping doc:
#
#   Flink TIMESTAMP => Avro: long + local-timestamp-millis / timestamp-millis
#   Flink TIME       => Avro: int + time-millis
#   Flink DATE       => Avro: int + date
#   Flink DECIMAL    => Avro: bytes + decimal(precision, scale)
#   Flink STRING     => Avro: string
#   Flink BYTES      => Avro: bytes
#   Flink INT        => Avro: int
#   Flink BIGINT     => Avro: long
#   Flink BOOLEAN    => Avro: boolean
#   Flink FLOAT      => Avro: float
#   Flink DOUBLE     => Avro: double
#   Flink ROW        => Avro: record
#   Flink MAP/ARRAY  => Avro: map/array
#
def python_datetime_to_millis(dt: datetime) -> int:
    """
    Convert a Python datetime to milliseconds since the Unix epoch.
    """
    # By definition, Unix epoch = 1970-01-01 00:00:00 UTC
    # Here we assume naive datetimes or local time, adjust if you handle time zones
    epoch = datetime(1970, 1, 1)
    delta = dt - epoch
    return int(delta.total_seconds() * 1000.0)


def python_time_to_millis(t: time) -> int:
    """
    Convert a Python time to milliseconds since midnight.
    """
    midnight = timedelta(
        hours=t.hour, minutes=t.minute, seconds=t.second, microseconds=t.microsecond
    )
    return int(midnight.total_seconds() * 1000)


def python_date_to_days(d: date) -> int:
    """
    Convert a Python date to days since Unix epoch (1970-01-01).
    """
    return (d - date(1970, 1, 1)).days


def millis_to_python_datetime(ms: int) -> datetime:
    """
    Convert milliseconds since Unix epoch to Python datetime.
    """
    return datetime(1970, 1, 1) + timedelta(milliseconds=ms)


def millis_to_python_time(ms: int) -> time:
    """
    Convert milliseconds since midnight to Python time.
    """
    secs, ms_rem = divmod(ms, 1000)
    microsecs = ms_rem * 1000
    hh, remainder = divmod(secs, 3600)
    mm, ss = divmod(remainder, 60)
    return time(hour=hh, minute=mm, second=ss, microsecond=microsecs)


def days_to_python_date(days: int) -> date:
    """
    Convert days since Unix epoch to Python date.
    """
    return date(1970, 1, 1) + timedelta(days=days)


def quantize_decimal(value: Decimal, precision: int, scale: int) -> Decimal:
    """
    Quantize decimal to the specified precision and scale.
    """
    if value is None:
        raise ValueError("Value cannot be None")
    context = getcontext().copy()
    context.prec = precision
    try:
        quantized = value.quantize(
            Decimal(f"1e-{scale}"), rounding=ROUND_HALF_EVEN, context=context
        )
        if len(str(quantized).replace(".", "").replace("-", "")) > precision:
            raise DecimalException(
                f"Value {value} exceeds precision {precision} and scale {scale}"
            )
        return quantized
    except DecimalException as e:
        raise DecimalException(
            f"Value {value} is invalid for precision {precision} and scale {scale}"
        ) from e


##############################################################################
#  Converters for Python -> Avro/Confluent (the numeric representation)      #
##############################################################################

def enum_converter(x, symbols, enum_type):
    """
    Convert x to an enum_type member.
    - If x is str, treat x as the enum member name (e.g. "RED").
    - If x is int, treat x as an index into symbols (0 -> "RED").
    """
    if x is None:
        return None
    
    if isinstance(x, str):
        # e.g. "RED" -> Color.RED
        return enum_type[x]  # relies on enum_type["RED"]
    
    if isinstance(x, int):
        # e.g. 0 -> "RED" -> Color.RED
        # NOTE: ensure x is within range
        if x < 0 or x >= len(symbols):
            raise ValueError(f"Invalid enum index {x} for symbols {symbols}")
        name = symbols[x]  # e.g. symbols[0] -> "RED"
        return enum_type[name]
    
    raise ValueError(f"Cannot convert {x} to enum {enum_type}")

def decimal_converter(x: Any, p: int, s: int) -> Decimal | None:
    """Convert any input x to a Decimal with precision/scale."""
    return quantize_decimal(Decimal(x), p, s) if x is not None else None


def datetime_converter_py_to_avro(x: datetime) -> int:
    """Python datetime -> long (millis since epoch)."""
    return python_datetime_to_millis(x)


def datetime_converter_avro_to_py(x: int) -> datetime:
    """long (millis) -> Python datetime."""
    return millis_to_python_datetime(x)


def time_converter_py_to_avro(x: time) -> int:
    """Python time -> int (millis since midnight)."""
    return python_time_to_millis(x)


def time_converter_avro_to_py(x: int) -> time:
    """int (millis) -> Python time."""
    return millis_to_python_time(x)


def date_converter_py_to_avro(x: date) -> int:
    """Python date -> int (days since epoch)."""
    return python_date_to_days(x)


def date_converter_avro_to_py(x: int) -> date:
    """int (days since epoch) -> Python date."""
    return days_to_python_date(x)


def record_converter(x: Any, schema: Any) -> Any:
    """Convert a nested record via the schema's deserialize if present."""
    if hasattr(schema, "deserialize"):
        return schema.deserialize(x)
    return x


##############################################################################
#   Table of Converters for "logicalType" usage (Python -> Avro numeric)      #
##############################################################################
CONVERTERS: Dict[str, Any] = {
    "decimal": decimal_converter,  # decimal(x, p, s)
    "time-millis": time_converter_py_to_avro,
    "date": date_converter_py_to_avro,
    "timestamp-millis": datetime_converter_py_to_avro,
    "local-timestamp-millis": datetime_converter_py_to_avro,  # same numeric representation
    "enum": enum_converter,
    "string": lambda x: str(x).strip(),
    "bytes": lambda x: x if isinstance(x, bytes) else bytes(str(x), "utf-8"),
    "int": int,
    "float": float,
    "boolean": lambda x: bool(
        int(x) if isinstance(x, str) and x.lower() in ["1", "true", "0", "false"] else x
    ),
    "null": lambda x: None,
    "record": record_converter,
}


@attrs.define
class SchemaField:
    """
    A custom field class that extends attrs.field with a metadata structure
    to keep track of Avro and Flink type info. 
    """
    alias: Optional[str] = None
    logical_type: Optional[str] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    default: Any = None
    enum_symbols: Optional[List[str]] = None
    enum_type: Optional[Type[Enum]] = None
    size: Optional[int] = None
    items: Optional[Any] = None
    values: Optional[Any] = None

    def __attrs_post_init__(self) -> None:
        pass

    def __call__(self) -> attrs.field:
        """
        Return an attrs field with proper metadata and (Python -> Avro) converter.
        """
        metadata: Dict[str, Any] = {}
        if self.alias:
            metadata["alias"] = self.alias

        if self.logical_type in CONVERTERS:
            converter = CONVERTERS[self.logical_type]
            metadata["logicalType"] = self.logical_type

            # For enumerations
            if self.logical_type == "enum":
                if not self.enum_symbols or not self.enum_type:
                    raise ValueError("Must provide enum_symbols and enum_type for 'enum'.")
                metadata["symbols"] = str(self.enum_symbols)
                return attrs.field(
                    default=self.default,
                    converter=lambda x: converter(x, self.enum_symbols, self.enum_type),
                    metadata=metadata,
                )

            # For decimal
            if self.logical_type == "decimal":
                if self.precision is None or self.scale is None:
                    raise ValueError("Decimal fields need precision and scale.")
                metadata["precision"] = self.precision
                metadata["scale"] = self.scale
                return attrs.field(
                    default=self.default,
                    converter=lambda x: converter(x, self.precision, self.scale),
                    metadata=metadata,
                )

            # For date/time/timestamp + basic
            return attrs.field(
                default=self.default,
                converter=converter,
                metadata=metadata,
            )

        # Possibly fixed, array, map
        if self.size:
            metadata["size"] = self.size
        if self.items:
            metadata["items"] = self.items
        if self.values:
            metadata["values"] = self.values

        return attrs.field(
            default=self.default,
            metadata=metadata,
        )


def generate_avro_schema(cls: Type[Any]) -> Optional[Dict[str, Any]]:
    """
    Dynamically generate Avro schema based on the class's fields, 
    respecting the logical types for Flink integration.
    """
    if cls.SchemaConfig._type == "avro":
        name = cls.SchemaConfig.name or "DefaultSchema"
        namespace = cls.SchemaConfig.namespace or None
        record_type = cls.SchemaConfig.type or "record"
        doc = cls.SchemaConfig.doc or None
        schema = {}
        schema["name"] = name
        schema["type"] = record_type
        if namespace:
            schema["namespace"] = namespace
        if doc:
            schema["doc"] = doc
        schema["fields"] = [
            {
                "name": f.metadata.get("alias", f.name),
                "type": infer_avro_type(f.type, f.metadata),
                "flink.type": infer_flink_type(f.type, f.metadata),
            }
            for f in attrs.fields(cls)
        ]
        return schema
    raise ValueError("Only Avro schema is supported for now.")


##############################################################################
#        Python -> Avro Type inference, respecting the doc's instructions    #
##############################################################################
def infer_avro_type(py_type: Type[Any], metadata: Dict[str, Any]) -> Any:
    """
    Given a Python type + metadata, produce the correct Avro field definition 
    (type + logicalType, etc.) that Flink can properly interpret.
    """
    logical_type: Optional[str] = metadata.get("logicalType")

    if logical_type == "decimal":
        # Avro representation: bytes + {"logicalType": "decimal", "precision": X, "scale": Y}
        precision = metadata["precision"]
        scale = metadata["scale"]
        return {
            "type": "bytes",
            "logicalType": "decimal",
            "precision": precision,
            "scale": scale,
        }
    elif logical_type == "time-millis":
        return {"type": "int", "logicalType": "time-millis"}
    elif logical_type in ("timestamp-millis", "local-timestamp-millis"):
        # We'll unify everything under local-timestamp-millis or timestamp-millis
        # for Flink TIMESTAMP(3).
        return {"type": "long", "logicalType": "local-timestamp-millis"}
    elif logical_type == "date":
        return {"type": "int", "logicalType": "date"}
    elif logical_type == "enum":
        # Some users produce Avro enum.  But Flink typically sees them as STRING.
        # If you want a pure Avro enum, do this:
        symbols_str = metadata.get("symbols", "[]")
        # parse or store as array if needed
        # For now, just store as Avro enum
        # Actually, Avro requires: { "type": "enum", "name": "SomeName", "symbols": [...] }
        # We'll assume "name" can be "EnumField"
        import ast
        try:
            symbols_list = ast.literal_eval(symbols_str)
        except Exception:
            symbols_list = []
        return {
            "type": "enum",
            "name": "EnumField",
            "symbols": symbols_list,
        }

    # If no known logical type
    avro_type: Dict[str, Any] = {}

    # Possibly fixed
    if "size" in metadata:
        avro_type = {
            "type": "fixed",
            "name": "fixed_field",
            "namespace": "io.confluent",
            "size": metadata["size"],
        }
        return avro_type

    # Possibly array
    if "items" in metadata:
        avro_type = {
            "type": "array",
            "items": infer_avro_type(metadata["items"], {}),
        }
        return avro_type

    # Possibly map
    if "values" in metadata:
        avro_type = {
            "type": "map",
            "values": infer_avro_type(metadata["values"], {}),
        }
        return avro_type

    # Check standard python -> Avro primitives
    if py_type is str:
        avro_type = {"type": "string"}
    elif py_type is int:
        avro_type = {"type": "int"}
    elif py_type is float:
        avro_type = {"type": "float"}
    elif py_type is bool:
        avro_type = {"type": "boolean"}
    elif py_type is bytes:
        avro_type = {"type": "bytes"}
    elif py_type is None:
        avro_type = {"type": "null"}
    elif py_type is datetime:
        # default to local-timestamp-millis
        avro_type = {"type": "long", "logicalType": "local-timestamp-millis"}
    elif py_type is date:
        avro_type = {"type": "int", "logicalType": "date"}
    elif py_type is time:
        avro_type = {"type": "int", "logicalType": "time-millis"}
    elif get_origin(py_type) is list:
        subtype = get_args(py_type)[0]
        avro_type = {
            "type": "array",
            "items": infer_avro_type(subtype, {}),
        }
    elif get_origin(py_type) is dict:
        # map with string key
        val_type = get_args(py_type)[1]
        avro_type = {
            "type": "map",
            "values": infer_avro_type(val_type, {}),
        }
    elif attrs.has(py_type):
        # nested record
        avro_type = generate_avro_schema(py_type)
    elif get_origin(py_type) is Union:
        # Build a list of valid Avro types
        union_members = []
        for t in get_args(py_type):
            if t is type(None):
            # Avro uses just the string "null" for the null type
                union_members.append("null")
            else:
                # Recurse to get a valid Avro type (string like "int" or dict)
                sub_schema = infer_avro_type(t, {})
                # If sub_schema is something like {"type": "string"}, we can either keep the dict
                # or just "string". Both are acceptable so long as it's a valid schema.
                # For primitive strings/ints, we can simplify to "string", "int", etc.
                if isinstance(sub_schema, dict) and "type" in sub_schema and len(sub_schema) == 1:
                    # e.g. {"type": "string"} => "string"
                    union_members.append(sub_schema["type"])
                else:
                    union_members.append(sub_schema)
        return union_members  # ✅ Return a list
    else:
        avro_type = {"type": "null"}

    return avro_type


##############################################################################
#   Python -> Flink Type inference, so we put "flink.type" in the schema     #
##############################################################################
def infer_flink_type(py_type: Type[Any], metadata: Dict[str, Any]) -> str:
    """
    Infer Flink SQL Type from Python type + metadata, consistent with the doc table.
    """
    logical_type: Optional[str] = metadata.get("logicalType")
    if logical_type == "decimal":
        precision = metadata["precision"]
        scale = metadata["scale"]
        return f"DECIMAL({precision}, {scale})"
    elif logical_type in ("timestamp-millis", "local-timestamp-millis"):
        # According to the doc, we map TIMESTAMP -> local-timestamp-millis, with precision up to 3
        return "TIMESTAMP(3)"
    elif logical_type == "time-millis":
        return "TIME(3)"
    elif logical_type == "date":
        return "DATE"
    elif logical_type == "enum":
        # Flink typically sees this as STRING
        return "STRING"

    # Possibly fixed => Varbinary or BINARY
    if "size" in metadata:
        # Could map to BINARY if you want a fixed length
        return f"VARBINARY({metadata['size']})"

    # Possibly array
    if "items" in metadata:
        subtype = metadata["items"]
        return f"ARRAY<{infer_flink_type(subtype, {})}>"

    # Possibly map
    if "values" in metadata:
        subtype = metadata["values"]
        return f"MAP<STRING, {infer_flink_type(subtype, {})}>"

    # Now try standard Python types
    if py_type is int:
        # Could be SMALLINT, TINYINT, etc., but let's default to INT
        return "INT"
    elif py_type is float:
        return "DOUBLE"  # or "FLOAT"; pick your preference
    elif py_type is bool:
        return "BOOLEAN"
    elif py_type is str:
        return "STRING"
    elif py_type is bytes:
        return "BYTES"  # or "VARBINARY"
    elif py_type is datetime:
        return "TIMESTAMP(3)"
    elif py_type is date:
        return "DATE"
    elif py_type is time:
        return "TIME(3)"
    elif attrs.has(py_type):
        # ROW type
        fields_def = []
        for f in attrs.fields(py_type):
            fields_def.append(f"{f.name} {infer_flink_type(f.type, f.metadata)}")
        return f"ROW<{', '.join(fields_def)}>"
    elif get_origin(py_type) is list:
        subtype = get_args(py_type)[0]
        return f"ARRAY<{infer_flink_type(subtype, {})}>"
    elif get_origin(py_type) is dict:
        val_type = get_args(py_type)[1]
        return f"MAP<STRING, {infer_flink_type(val_type, {})}>"
    elif get_origin(py_type) is Union:
        # e.g. Optional or union
        # We'll just pick the first non-None for a best guess
        union_types = [t for t in get_args(py_type) if t is not type(None)]
        if not union_types:
            return "STRING"
        # pick the first for demonstration
        return infer_flink_type(union_types[0], {})
    return "STRING"


class SchemaConfig:
    """
    Simple schema config object. We only support Avro for now.
    """
    _type: Literal["avro"] = "avro"
    name: str = "DefaultSchema"
    namespace: str = ""
    doc: str = ""
    type: str = "record"



T = TypeVar('T')


class AvroSchema(Generic[T]):
    """Base class for Avro schema with serialization/deserialization methods."""
    
    def to_dict(self, *args, **kwargs) -> Dict[str, Any]:
        """Convert the instance to a dictionary representation."""
        raise NotImplementedError("Subclasses must implement this method.")

    @classmethod
    def from_dict(cls, data: Dict[str, Any], *args, **kwargs) -> 'AvroSchema':
        """Create an instance from a dictionary representation."""
        raise NotImplementedError("Subclasses must implement this method.")


def AvroModel(cls: Type[T]) -> Type[AvroSchema]:
    """
    Decorator that transforms a class into an attrs-defined class with
    additional Avro schema/serde methods for Flink round-trip.

    Args:
        cls (Type[T]): The class to be transformed.

    Returns:
        Type[T]: The transformed class with Avro schema methods.

    Example:
        class MyClass(AvroSchema):
            pass
    """
    return schema(cls)


def schema(cls: Type[T]) -> Type[AvroSchema]:
    """
    Decorator that transforms a class into an attrs-defined class with
    additional Avro schema/serde methods for Flink round-trip.
    """
    cls = attrs.define(cls)
    if not hasattr(cls, "SchemaConfig"):
        cls.SchemaConfig = SchemaConfig()  # type: ignore
    cls.SchemaConfig._type = "avro"  # type: ignore

    # Dynamically build Avro schema
    cls.schema_dict = property(lambda self: generate_avro_schema(cls))  # type: ignore
    cls.schema_str = property(lambda self: json.dumps(self.schema_dict))  # type: ignore
    cls.schema = property(lambda self: Schema(self.schema_str, "AVRO"))
    # Convert to dictionary (Python object -> "plain" dict),
    # storing date/time/datetime as numeric where relevant.
    def to_dict(self_: Any, *args, **kwargs) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for f in attrs.fields(cls):  # type: ignore
            val = getattr(self_, f.name)
            lt = f.metadata.get("logicalType")
            if val is None:
                out[f.name] = None
            elif lt == "timestamp-millis" or lt == "local-timestamp-millis":
                # store as ms since epoch
                if isinstance(val, datetime):
                    out[f.name] = python_datetime_to_millis(val)
                else:
                    out[f.name] = val
            elif lt == "time-millis":
                # store as ms since midnight
                if isinstance(val, time):
                    out[f.name] = python_time_to_millis(val)
                else:
                    out[f.name] = val
            elif lt == "date":
                if isinstance(val, date):
                    out[f.name] = python_date_to_days(val)
                else:
                    out[f.name] = val
            elif lt == "enum":
                # If we truly want Avro enumerations, store the name
                if isinstance(val, Enum):
                    out[f.name] = val.name
                else:
                    out[f.name] = str(val)
            else:
                # fallback
                if isinstance(val, Enum):
                    # By default, store enum as its name
                    out[f.name] = val.name
                else:
                    out[f.name] = val
        return out

    cls.to_dict = to_dict  # Assign the to_dict method to the class

    # from_dict for easy structure
    def from_dict(cls_, data: Dict[str, Any]) -> Any:
        # We'll run a post-processing step if we see date/time/datetime
        # to convert from numeric to python objects
        def structure_converter(val: Any, field) -> Any:
            lt = field.metadata.get("logicalType")
            if lt in ("timestamp-millis", "local-timestamp-millis"):
                if val is None:
                    return None
                return millis_to_python_datetime(val)
            elif lt == "time-millis":
                if val is None:
                    return None
                return millis_to_python_time(val)
            elif lt == "date":
                if val is None:
                    return None
                return days_to_python_date(val)
            elif lt == "enum":
                # If we want to parse to an actual Enum:
                enum_symbols = field.metadata.get("symbols")
                enum_type = field.metadata.get("enum_type")
                if enum_symbols and enum_type and val is not None:
                    # e.g. "RED" -> Color.RED
                    try:
                        return enum_type[val]
                    except KeyError:
                        # fallback if it doesn't match
                        pass
                return val
            else:
                # fallback
                return val

        # cattrs approach: we can do a simple pass here
        # We'll produce a new dict with the correct structured values
        structured_data: Dict[str, Any] = {}
        for f in attrs.fields(cls_):
            if f.name not in data:
                structured_data[f.name] = None
                continue
            structured_data[f.name] = structure_converter(data[f.name], f)

        return cls_(**structured_data)

    cls.from_dict = classmethod(from_dict)  # Assign from_dict as a class method
    
    # to_bytes / from_bytes using Confluent Avro
    def to_bytes(self_, registry_client: SchemaRegistryClient, subject: str) -> bytes:
        """
        Convert this instance into Avro-serialized bytes using Confluent AvroSerializer.
        Ensuring date/time/datetime are numeric.
        """
        schema_dict = generate_avro_schema(cls)
        if not schema_dict:
            raise ValueError("No Avro schema found.")
        schema_str = json.dumps(schema_dict)

        avro_serializer = AvroSerializer(
            schema_registry_client=registry_client,
            schema_str=schema_str,
        )
        serialized = avro_serializer(
            self_.to_dict(),
            SerializationContext(topic=subject, field=MessageField.VALUE),
        )
        return serialized

    cls.to_bytes = to_bytes

    def from_bytes(cls_, data: bytes, registry_client: SchemaRegistryClient, subject: str) -> Any:
        """
        Deserialize Avro-serialized bytes into an instance of this class,
        ensuring numeric date/time/datetime are converted back to Python objects.
        """
        schema_dict = generate_avro_schema(cls_)
        if not schema_dict:
            raise ValueError("No Avro schema found.")
        schema_str = json.dumps(schema_dict)

        avro_deserializer = AvroDeserializer(
            schema_registry_client=registry_client,
            schema_str=schema_str,
            from_dict=lambda obj, _: cls_.from_dict(obj),
        )
        result = avro_deserializer(
            data, SerializationContext(topic=subject, field=MessageField.VALUE)
        )
        return result

    cls.from_bytes = classmethod(from_bytes)

    return cls



