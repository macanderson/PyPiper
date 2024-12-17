import attrs
from attrs import field, fields
from datetime import datetime, date, time
from decimal import Decimal, DecimalException, getcontext, ROUND_HALF_EVEN
from typing import Any, Dict, List, Optional, Type, Union, get_args, get_origin
from enum import Enum
import json

__all__ = ['StreamingModelSettings', 'StreamingField', 'streaming_data']

# Avro Primitive Types
AVRO_PRIMITIVES = {
    str: "string",
    int: "int",
    float: "float",
    bool: "boolean",
    bytes: "bytes"
}

# Flink Type Mappings from Python Types
FLINK_PRIMITIVE_TYPES = {
    str: "STRING",
    int: "INT",
    float: "DOUBLE",
    bool: "BOOLEAN",
    bytes: "BYTES",
    datetime: "TIMESTAMP(3)",
    date: "DATE"
}

FLINK_LOGICAL_TYPE_BASE = {
    "decimal": "BYTES",
    "timestamp-millis": "TIMESTAMP(3)",
    "date": "DATE",
    "time-millis": "TIME"
}

LOGICAL_TYPE_BASE = {
    "decimal": "bytes",
    "timestamp-millis": "long",
    "date": "int",
    "time-millis": "int"
}


@attrs.define(frozen=True)
class StreamingModelSettings:
    """Kafka Model Settings."""
    default_topic: str = "default_topic"
    avro_schema_name: str = "default_schema_name"
    avro_schema_namespace: str = "default_schema_namespace"
    avro_schema_doc: str = "default_schema_doc"
    num_partitions: int = 1
    replication_factor: int = 1
    min_insync_replicas: int = 1
    retention_ms: int = 2592000000  # 30 days in milliseconds
    retention_bytes: int = -1
    cleanup_policy: str = "delete"
    key_field: str = "key"
    value_field: str = "value"
    excluded_fields: List[str] = []
    header_fields: List[str] = []
    timestamp_field: str = "timestamp"
    # assume timestamp type is record append time not created time if the timestamp field is not present
    timestamp_type: str = "record_append_time" if timestamp_field else "created_time"

def quantize_decimal(value: Decimal, precision: int, scale: int) -> Decimal:
    """Quantize decimal to given precision and scale."""
    if value is None:
        return None
    context = getcontext().copy()
    context.prec = precision
    try:
        quantized = value.quantize(Decimal(f"1e-{scale}"), rounding=ROUND_HALF_EVEN, context=context)
        if len(str(quantized).replace(".", "").replace("-", "")) > precision:
            raise DecimalException(f"Value {value} exceeds precision {precision} and scale {scale}")
        return quantized
    except DecimalException as e:
        raise DecimalException(f"Value {value} is invalid for precision {precision} and scale {scale}") from e

CONVERTERS = {
    "decimal": lambda x, p, s: quantize_decimal(Decimal(x), p, s),
    "time-millis": lambda x: int((datetime.combine(date.min, x) - datetime.min).total_seconds() * 1000),
    "date": lambda x: (date.fromisoformat(x) - date(1970, 1, 1)).days if isinstance(x, str) else (x - date(1970, 1, 1)).days,
    "timestamp-millis": lambda x: int(datetime.fromisoformat(x).timestamp() * 1000) if isinstance(x, str) else int(x.timestamp() * 1000),
    "enum": lambda x, symbols, enum_type: enum_type[symbols.index(x)] if isinstance(x, str) else enum_type[x],
    "string": lambda x: str(x).strip(),
    "bytes": lambda x: bytes(x, "utf-8") if isinstance(x, str) else x,
    "int": lambda x: int(x),
    "float": lambda x: float(x),
    "boolean": lambda x: bool(int(x) if isinstance(x, str) and x.lower() in ["1", "true", "0", "false"] else x),
    "null": lambda x: None,
    "record": lambda x, schema: schema.deserialize(x) if hasattr(schema, 'deserialize') else x,
}

class StreamingField:
    """Custom Field that extends attrs.field with readable properties."""
    def __init__(
        self,
        alias: Optional[str] = None,
        logical_type: Optional[str] = None,
        precision: Optional[int] = None,
        scale: Optional[int] = None,
        default: Any = attrs.NOTHING,
        enum_symbols: Optional[List[str]] = None,
        enum_type: Optional[Type[Enum]] = None,
        size: Optional[int] = None,
        items: Optional[Any] = None,
        values: Optional[Any] = None
    ):
        self.alias = alias
        self.logical_type = logical_type
        self.precision = precision
        self.scale = scale
        self.default = default
        self.enum_symbols = enum_symbols
        self.enum_type = enum_type
        self.size = size
        self.items = items
        self.values = values

    def to_attrs_field(self):
        metadata = {"alias": self.alias} if self.alias else {}
        if self.logical_type in CONVERTERS:
            converter = CONVERTERS[self.logical_type]
            if self.logical_type == "enum":
                metadata["symbols"] = self.enum_symbols
                return field(
                    default=self.default,
                    converter=lambda x: converter(x, self.enum_symbols, self.enum_type),
                    metadata=metadata
                )
            else:
                metadata["logicalType"] = self.logical_type
                if self.logical_type == "decimal":
                    metadata.update({"precision": self.precision, "scale": self.scale})
                    return field(
                        default=self.default,
                        converter=lambda x: converter(x, self.precision, self.scale),
                        metadata=metadata
                    )
                else:
                    return field(
                        default=self.default,
                        converter=converter,
                        metadata=metadata
                    )
        elif self.size:
            metadata["size"] = self.size
            return field(default=self.default, metadata=metadata)
        elif self.items:
            metadata["items"] = self.items
        elif self.values:
            metadata["values"] = self.values
        return field(default=self.default, metadata=metadata)

def infer_avro_type(py_type: Type, metadata: Dict) -> Any:
    """Infer Avro type and additional properties based on Python type and metadata."""
    logical_type = metadata.get("logicalType")
    if logical_type:
        return {"type": FLINK_LOGICAL_TYPE_BASE.get(logical_type, "null"), **metadata}

    avro_type = {}
    if "symbols" in metadata:  # Enum
        avro_type = {"type": "enum", "symbols": metadata["symbols"]}
    elif "size" in metadata:  # Fixed
        avro_type = {"type": "fixed", "size": metadata["size"]}
    elif "items" in metadata:  # Array
        avro_type = {"type": "array", "items": infer_avro_type(metadata["items"], {})}
    elif "values" in metadata:  # Map
        avro_type = {"type": "map", "values": infer_avro_type(metadata["values"], {})}

    if not avro_type:
        if py_type in AVRO_PRIMITIVES:  # Primitive types
            avro_type = {"type": AVRO_PRIMITIVES[py_type]}
        elif get_origin(py_type) is list:  # Array
            avro_type = {"type": "array", "items": infer_avro_type(get_args(py_type)[0], {})}
        elif get_origin(py_type) is dict:  # Map
            avro_type = {"type": "map", "values": infer_avro_type(get_args(py_type)[1], {})}
        elif isinstance(py_type, type) and issubclass(py_type, Enum):  # Enum
            avro_type = {"type": "enum", "symbols": [e.name for e in py_type]}
        elif attrs.has(py_type):  # Record
            avro_type = make_avro_schema(py_type)
        elif get_origin(py_type) is Union:  # Union
            types = get_args(py_type)
            avro_type = {"type": "union", "types": [infer_avro_type(t, {}) for t in types]}
        else:
            avro_type = {"type": "null"}

    return avro_type


def infer_flink_type(py_type: Type, metadata: Dict) -> Any:
    """Infer Flink SQL type from Python type and metadata."""
    logical_type = metadata.get("logicalType")
    if logical_type == "decimal":
        precision, scale = metadata["precision"], metadata["scale"]
        return f"DECIMAL({precision}, {scale})"
    elif py_type in FLINK_PRIMITIVE_TYPES:
        return FLINK_PRIMITIVE_TYPES[py_type]
    elif get_origin(py_type) == List:
        return f"ARRAY<{infer_flink_type(get_args(py_type)[0], {})}>"
    elif get_origin(py_type) == Dict:
        return f"MAP<STRING, {infer_flink_type(get_args(py_type)[1], {})}>"
    elif attrs.has(py_type):
        return f"ROW<{', '.join(f'{f.name} {infer_flink_type(f.type, f.metadata)}' for f in fields(py_type))}>"
    elif isinstance(py_type, Enum):
        return f"ENUM<{', '.join([e.name for e in py_type])}>"
    return "UNKNOWN"

def make_avro_schema(cls):
    """Generate Avro schema dynamically for the given class."""
    return {
        "type": "record",
        "name": cls.__name__,
        "fields": [
            {
                "name": f.metadata.get("alias", f.name),
                "type": infer_avro_type(f.type, f.metadata),
                "flink.type": infer_flink_type(f.type, f.metadata)
            } 
            for f in fields(cls)
        ]
    }



def streaming_data(cls):
    """Kafka Model Decorator."""
    cls = attrs.define(cls)
    cls.avro_schema = property(lambda self: make_avro_schema(cls))
    cls.Settings = type('StreamingModelSettings', (), {})  # Default empty StreamingModelSettings class
    return cls

# Example Enum
class Color(Enum):
    RED = 1
    GREEN = 2
    BLUE = 3

# Example Usage
@streaming_data
class ExampleModel:
    """Example Kafka Model."""
    field1: str = StreamingField(alias="f1", default="default_value").to_attrs_field()
    field2: int = StreamingField(default=42).to_attrs_field()
    field3: Decimal = StreamingField(
        logical_type="decimal", precision=6, scale=2, default=Decimal("0.00")
    ).to_attrs_field()
    field4: Optional[str] = StreamingField(default=None).to_attrs_field()
    field5: Color = StreamingField(enum_symbols=["RED", "GREEN", "BLUE"], enum_type=Color, default=Color.RED).to_attrs_field()
    field6: List[int] = StreamingField(items=int, default=[]).to_attrs_field()
    field7: Union[int, str] = StreamingField(default='42').to_attrs_field()
    field8: Union[int, Dict[str, Any]] = StreamingField(default=42).to_attrs_field()
    field9: datetime = StreamingField(logical_type="timestamp-millis", default=datetime.now()).to_attrs_field()
    field10: date = StreamingField(logical_type="date", default=datetime.now().date()).to_attrs_field()
    field11: time = StreamingField(logical_type="time-millis", default=datetime.now().time()).to_attrs_field()
    field12: Dict[str, Any] = StreamingField(default={}).to_attrs_field()
    field13: bytes = StreamingField(logical_type="fixed", size=5, default=b"hello").to_attrs_field()
    
    class Settings:
        default_topic = "example_topic"
        avro_schema_name = "example_schema override"
        avro_schema_namespace = "example_schema_namespace override"
        avro_schema_doc = "example_schema_doc override"
        num_partitions = 2
        replication_factor = 1
        min_insync_replicas = 1
        retention_ms = 2592000000  # 30 days in milliseconds
        retention_bytes = -1
        cleanup_policy = "delete"

if __name__ == "__main__":
    instance = ExampleModel()
    print("Avro Schema:")
    print(json.dumps(instance.avro_schema, indent=2))

    print("\nInstance Data:")
    print(attrs.asdict(instance))

    try:  # this will fail
        instance.field3 = Decimal("12345.1234")
        print("\nUpdated Decimal Field3:", instance.field3)
    except DecimalException as e:
        print("\nError:", e)
    
    try:  # this will work
        instance.field3 = Decimal("125.1234")
        print("\nUpdated Field3:", instance.field3)
    except DecimalException as e:
        print("\nError:", e)

    try:  # this will fail
        instance.field5 = "RED"
        print("\nUpdated Field5:", instance.field5)
    except ValueError as e:
        print("\nError:", e)

    try:  # this will work
        instance.field5 = Color.RED
        print("\nUpdated Field5:", instance.field5)
    except ValueError as e:
        print("\nError:", e)

    try:  # this won't work invalid iso string format
        instance.field9 = '2023-01-01 2:30:00'
        print("\nUpdated Field5:", instance.field5)
    except ValueError as e:
        print("\nError:", e)

    try:  # this will work
        instance.field9 = datetime(2023, 1, 1, 12, 30).strftime("%Y-%m-%d %H:%M:%S") ## this will work as will any other valid iso string format
        print("\nUpdated DateTime Field9:", instance.field9)
    except ValueError as e:
        print("\nError:", e)

    # Testing date, time, and datetime fields
    try:  # this will work
        instance.field10 = date(2023, 1, 1)
        print("\nUpdated Date Field10:", instance.field10)
    except ValueError as e:
        print("\nError:", e)

    try:  # this will work
        instance.field11 = time(12, 30)
        print("\nUpdated Time Field11:", instance.field11)
    except ValueError as e:
        print("\nError:", e)

    try:  # this will work
        instance.field9 = datetime(2023, 1, 1, 12, 30)
        print("\nUpdated DateTime Field9:", instance.field9)
    except ValueError as e:
        print("\nError:", e)

    print("\nUpdated Field13:", instance.field13)

    print("\nUpdated Field12:", instance.field12)

    print("\nUpdated Field11:", instance.field11)

    print("\nUpdated Field10:", instance.field10)

    print("\nUpdated Field9:", instance.field9)

    print("\nUpdated Field8:", instance.field8)

    print("\nUpdated Field7:", instance.field7)

    print("\nUpdated Field6:", instance.field6)

    print("\nUpdated Field5:", instance.field5)

    print("\nUpdated Field4:", instance.field4)

    print("\nUpdated Field3:", instance.field3)

    print("\nUpdated Field2:", instance.field2)

    print("\nUpdated Field1:", instance.field1)
