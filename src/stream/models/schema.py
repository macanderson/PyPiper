"""Create avro schema enabled models as an extension of attrs."""
import json

from datetime import datetime, date, time, timedelta
from decimal import (
    Decimal,
    DecimalException,
    getcontext,
    ROUND_HALF_EVEN,
    InvalidOperation
)
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
from enum import Enum, StrEnum, IntEnum

import attrs

from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Date/Time Helpers & Converters
def python_datetime_to_millis(dt: datetime) -> int:
    """Convert a Python datetime to milliseconds since the Unix epoch."""
    epoch = datetime(1970, 1, 1)
    delta = dt - epoch
    return int(delta.total_seconds() * 1000.0)

def python_time_to_millis(t: time) -> int:
    """Convert a Python time to milliseconds since midnight."""
    midnight = timedelta(
        hours=t.hour, minutes=t.minute, seconds=t.second, microseconds=t.microsecond
    )
    return int(midnight.total_seconds() * 1000)

def python_date_to_days(d: date) -> int:
    """Convert a Python date to days since Unix epoch (1970-01-01)."""
    return (d - date(1970, 1, 1)).days

def millis_to_python_datetime(ms: int) -> datetime:
    """Convert milliseconds since Unix epoch to Python datetime."""
    return datetime(1970, 1, 1) + timedelta(milliseconds=ms)

def millis_to_python_time(ms: int) -> time:
    """Convert milliseconds since midnight to Python time."""
    secs, ms_rem = divmod(ms, 1000)
    microsecs = ms_rem * 1000
    hh, remainder = divmod(secs, 3600)
    mm, ss = divmod(remainder, 60)
    return time(hour=hh, minute=mm, second=ss, microsecond=microsecs)

def days_to_python_date(days: int) -> date:
    """Convert days since Unix epoch to Python date."""
    return date(1970, 1, 1) + timedelta(days=days)

def quantize_decimal(value: Decimal, precision: int, scale: int) -> Decimal:
    """Quantize decimal to the specified precision and scale."""
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

def enum_converter(x, symbols, enum_type):
    """Convert x to an enum_type member."""
    if x is None:
        return None
    
    if isinstance(x, str):
        return enum_type[x]
    
    if isinstance(x, int):
        if x < 0 or x >= len(symbols):
            raise ValueError(f"Invalid enum index {x} for symbols {symbols}")
        name = symbols[x]
        return enum_type[name]
    
    if isinstance(x, Enum):
        if isinstance(x, StrEnum):
            return x.value
        elif isinstance(x, IntEnum):
            return str(x.value)
        return x.name
    
    raise ValueError(f"Cannot convert {x} to enum {enum_type}")

def decimal_converter(x: Any, p: int, s: int) -> Optional[Decimal]:
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

CONVERTERS: Dict[str, Any] = {
    "decimal": decimal_converter,
    "time-millis": time_converter_py_to_avro,
    "date": date_converter_py_to_avro,
    "timestamp-millis": datetime_converter_py_to_avro,
    "local-timestamp-millis": datetime_converter_py_to_avro,
    "enum": enum_converter,
    "string": lambda x: str(x).strip(),
    "bytes": lambda x: x if isinstance(x, bytes) else bytes(str(x), "utf-8"),
    "int": int,
    "float": float,
    "boolean": attrs.converters.to_bool,
    "null": lambda x: None,
    "record": record_converter,
}

@attrs.define
class SchemaField:
    """A custom field class that extends attrs.field with a metadata structure."""
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
        """Return an attrs field with proper metadata and (Python -> Avro) converter."""
        metadata: Dict[str, Any] = {}
        if self.alias:
            metadata["alias"] = self.alias

        if self.logical_type in CONVERTERS:
            converter = CONVERTERS[self.logical_type]
            metadata["logicalType"] = self.logical_type

            if self.logical_type == "enum":
                if not self.enum_symbols or not self.enum_type:
                    raise ValueError("Must provide enum_symbols and enum_type for 'enum'.")
                metadata["symbols"] = self.enum_symbols
                return attrs.field(
                    default=self.default,
                    converter=lambda x: converter(x, self.enum_symbols, self.enum_type),
                    metadata=metadata,
                )

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

            return attrs.field(
                default=self.default,
                converter=converter,
                metadata=metadata,
            )

        if self.size:
            metadata["size"] = self.size
        if self.items:
            metadata["items"] = self.items
            return attrs.field(
                type=List[self.items],
                default=self.default,
                metadata=metadata,
            )
        if self.values:
            metadata["values"] = self.values

        return attrs.field(
            default=self.default,
            metadata=metadata,
        )

def generate_avro_schema(cls: Type[Any]) -> Optional[Dict[str, Any]]:
    """Dynamically generate Avro schema based on the class's fields."""
    if cls.SchemaConfig._type == "avro":
        name = cls.SchemaConfig.name or "DefaultSchema"
        namespace = cls.SchemaConfig.namespace or None
        record_type = cls.SchemaConfig.type or "record"
        doc = cls.SchemaConfig.doc or None
        schema = {
            "name": name,
            "type": record_type,
            "namespace": namespace,
            "doc": doc,
            "fields": [
                {
                    "name": f.metadata.get("alias", f.name),
                    "type": infer_avro_type(f.type, f.metadata),
                    "flink.type": infer_flink_type(f.type, f.metadata),
                }
                for f in attrs.fields(cls)
            ],
        }
        return schema
    raise ValueError("Only Avro schema is supported for now.")

def infer_avro_type(py_type: Type[Any], metadata: Dict[str, Any]) -> Any:
    """Given a Python type + metadata, produce the correct Avro field definition."""
    logical_type: Optional[str] = metadata.get("logicalType")

    if logical_type == "decimal":
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
        return {"type": "long", "logicalType": "local-timestamp-millis"}
    elif logical_type == "date":
        return {"type": "int", "logicalType": "date"}
    elif logical_type == "enum":
        symbols_str = metadata.get("symbols", "[]")
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

    avro_type: Dict[str, Any] = {}

    if "size" in metadata:
        avro_type = {
            "type": "fixed",
            "name": "fixed_field",
            "namespace": "io.confluent",
            "size": metadata["size"],
        }
        return avro_type

    if "items" in metadata:
        avro_type = {
            "type": "array",
            "items": infer_avro_type(metadata["items"], {}),
        }
        return avro_type

    if "values" in metadata:
        avro_type = {
            "type": "map",
            "values": infer_avro_type(metadata["values"], {}),
        }
        return avro_type

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
        val_type = get_args(py_type)[1]
        avro_type = {
            "type": "map",
            "values": infer_avro_type(val_type, {}),
        }
    elif attrs.has(py_type):
        avro_type = generate_avro_schema(py_type)
    elif get_origin(py_type) is Union:
        union_members = []
        for t in get_args(py_type):
            if t is type(None):
                union_members.append("null")
            else:
                sub_schema = infer_avro_type(t, {})
                if isinstance(sub_schema, dict) and "type" in sub_schema and len(sub_schema) == 1:
                    union_members.append(sub_schema["type"])
                else:
                    union_members.append(sub_schema)
        return union_members
    else:
        avro_type = {"type": "null"}

    return avro_type

def infer_flink_type(py_type: Type[Any], metadata: Dict[str, Any]) -> str:
    """Infer Flink SQL Type from Python type + metadata."""
    logical_type: Optional[str] = metadata.get("logicalType")
    if logical_type == "decimal":
        precision = metadata["precision"]
        scale = metadata["scale"]
        return f"DECIMAL({precision}, {scale})"
    elif logical_type in ("timestamp-millis", "local-timestamp-millis"):
        return "TIMESTAMP(3)"
    elif logical_type == "time-millis":
        return "TIME(3)"
    elif logical_type == "date":
        return "DATE"
    elif logical_type == "enum":
        return "STRING"

    if "size" in metadata:
        return f"VARBINARY({metadata['size']})"

    if "items" in metadata:
        subtype = metadata["items"]
        return f"ARRAY<{infer_flink_type(subtype, {})}>"

    if "values" in metadata:
        subtype = metadata["values"]
        return f"MAP<STRING, {infer_flink_type(subtype, {})}>"

    if py_type is int:
        return "INT"
    elif py_type is float:
        return "DOUBLE"
    elif py_type is bool:
        return "BOOLEAN"
    elif py_type is str:
        return "STRING"
    elif py_type is bytes:
        return "BYTES"
    elif py_type is datetime:
        return "TIMESTAMP(3)"
    elif py_type is date:
        return "DATE"
    elif py_type is time:
        return "TIME(3)"
    elif attrs.has(py_type):
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
        union_types = [t for t in get_args(py_type) if t is not type(None)]
        if not union_types:
            return "STRING"
        return infer_flink_type(union_types[0], {})
    return "STRING"

class SchemaConfig:
    """Simple schema config object. We only support Avro for now."""
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
    """Decorator that transforms a class into an attrs-defined class with Avro schema methods."""
    return schema(cls)

def schema(cls: Type[T]) -> Type[AvroSchema]:
    """Decorate classes with `@schema` to enable flink/kafka round-trip."""
    cls = attrs.define(cls)
    if not hasattr(cls, "SchemaConfig"):
        cls.SchemaConfig = SchemaConfig()  # type: ignore
    cls.SchemaConfig._type = "avro"  # type: ignore

    cls.schema_dict = property(lambda self: generate_avro_schema(cls))  # type: ignore
    cls.schema_str = property(lambda self: json.dumps(self.schema_dict))  # type: ignore
    cls.schema = property(lambda self: Schema(self.schema_str, "AVRO"))  # type: ignore

    def to_dict(self_: Any, *args, **kwargs) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for f in attrs.fields(cls):  # type: ignore
            val = getattr(self_, f.name)
            logical_type = f.metadata.get("logicalType")
            if val is None:
                out[f.name] = None
            elif logical_type in ("timestamp-millis", "local-timestamp-millis"):
                if isinstance(val, datetime):
                    out[f.name] = int(val.timestamp() * 1000)
                else:
                    out[f.name] = int(val)
            elif logical_type == "time-millis":
                if isinstance(val, time):
                    out[f.name] = (val.hour * 3600000 + 
                                   val.minute * 60000 + 
                                   val.second * 1000 + 
                                   val.microsecond // 1000)
                else:
                    out[f.name] = int(val)
            elif logical_type == "date":
                if isinstance(val, date):
                    out[f.name] = python_date_to_days(val)
                else:
                    out[f.name] = int(val)
            elif logical_type == "enum":
                if isinstance(val, Enum):
                    out[f.name] = val.value.lower() if isinstance(val.value, str) else val.name.lower()
                else:
                    out[f.name] = str(val).lower()
            elif logical_type == "decimal":
                precision = f.metadata.get("precision", 10)
                scale = f.metadata.get("scale", 2)
                
                # Convert to big endian byte array
                if isinstance(val, (int, float, Decimal)):
                    decimal_val = Decimal(val)
                    scaled_val = decimal_val * (Decimal(10) ** scale)
                    byte_val = scaled_val.to_bytes((precision + 7) // 8, byteorder='big', signed=True)
                    out[f.name] = byte_val
                else:
                    out[f.name] = val
        return out

    cls.to_dict = to_dict  # type: ignore

    def from_dict(cls_, data: Dict[str, Any]) -> Any:
        """Convert a dictionary to an instance of the class."""
        def structure_converter(val: Any, field) -> Any:
            logical_type = field.metadata.get("logicalType")
            if logical_type in ("timestamp-millis", "local-timestamp-millis"):
                if val is None:
                    return None
                return millis_to_python_datetime(val)
            elif logical_type == "time-millis":
                if val is None:
                    return None
                return millis_to_python_time(val)
            elif logical_type == "date":
                if val is None:
                    return None
                return days_to_python_date(val)
            elif logical_type == "enum":
                enum_symbols = field.metadata.get("symbols")
                enum_type = field.metadata.get("enum_type")
                if enum_symbols and enum_type and val is not None:
                    try:
                        return enum_type[val]
                    except KeyError:
                        pass
                return val
            elif logical_type == "decimal":
                if val is None:
                    return None
                scale = field.metadata.get("scale", 2)
                
                # Convert big endian byte array to Decimal
                if isinstance(val, bytes):
                    decimal_val = int.from_bytes(val, byteorder='big', signed=True)
                    return Decimal(decimal_val) / (Decimal(10) ** scale)
                return val
            else:
                return val

        structured_data: Dict[str, Any] = {}
        for f in attrs.fields(cls_):
            if f.name not in data:
                structured_data[f.name] = None
                continue
            structured_data[f.name] = structure_converter(data[f.name], f)

        return cls_(**structured_data)

    cls.from_dict = classmethod(from_dict)  # type: ignore

    def to_bytes(self_, registry_client: SchemaRegistryClient, subject: str) -> bytes | None:
        """Convert this instance into Avro-serialized bytes using Confluent AvroSerializer."""
        schema_dict = generate_avro_schema(self_.__class__)
        if not schema_dict:
            raise ValueError("No Avro schema found.")
        schema_str = json.dumps(schema_dict)

        avro_serializer = AvroSerializer(
            schema_registry_client=registry_client,
            schema_str=schema_str,
        )
        
        data_dict = self_.to_dict()
        for key, field in attrs.fields(self_.__class__):
            value = getattr(self_, field.name)
            if isinstance(value, Decimal):
                metadata = field.metadata
                precision = metadata.get('precision')
                scale = metadata.get('scale')
                if precision is None or scale is None:
                    raise ValueError("Precision or scale not defined for decimal field.")
                try:
                    data_dict[key] = decimal_to_bytes(value, precision, scale)
                except ValueError as e:
                    raise

        serialized = avro_serializer(
            data_dict,
            SerializationContext(topic=subject, field=MessageField.VALUE),
        )
        return serialized

    cls.to_bytes = to_bytes  # type: ignore

    def from_bytes(cls_, data: bytes, registry_client: SchemaRegistryClient, subject: str) -> Any:
        """Deserialize Avro-serialized bytes into an instance of this class."""
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

    cls.from_bytes = classmethod(from_bytes)  # type: ignore

    return cls  # type: ignore

def decimal_to_bytes(value: Decimal, precision: int, scale: int) -> bytes:
    """Convert a Decimal value to a byte array in big endian format."""
    try:
        quantized_value = value.quantize(Decimal(f'1e-{scale}'))
        if len(quantized_value.as_tuple().digits) > precision:
            raise ValueError(f"Decimal value {value} exceeds precision {precision} and scale {scale}")
        return quantized_value.to_integral_value().to_bytes((quantized_value.adjusted() + 1 + 3) // 4, byteorder='big', signed=True)
    except InvalidOperation as e:
        raise ValueError(f"Error in decimal conversion for value {value} with precision {precision} and scale {scale}: {e}")

