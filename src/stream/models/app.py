"""App class for Kafka producer and consumer."""
from typing import Optional, Callable, Literal, Any, Dict

import attrs
import logging

from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.serialization import (
    StringSerializer, IntegerSerializer, DoubleSerializer,
    StringDeserializer, IntegerDeserializer, DoubleDeserializer
)

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.admin import AdminClient

from src.stream.models.schema import AvroSchema, AvroModel
from src.stream.config.settings import Settings


# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    encoding='utf-8',
    filename='app.log',
    filemode='a',
)

"""Import Settings loaded from a .env file."""
settings = Settings()

# Custom type kafka message key field serializer types
KeySerializerType = Literal['string', 'integer', 'double'] | None
# Custom type for kafka message value field serializer types
ValueSerializerType = Literal['avro', 'string', 'integer', 'double'] | None

# 
logger = logging.getLogger(__name__)

@attrs.define
class Pipeline:
    """Producer for Avro using settings for authentication.

    Summary:
        Producer for Avro using settings for authentication.

    Attributes:
        producer: SerializingProducer
        schema_registry_client: SchemaRegistryClient
        admin_client: AdminClient
    """
    id: str
    input_type: Literal['custom']
    input_name: str
    input_schema: Optional[AvroSchema]
    output_type: Literal['kafka', 'flink', 'kafka_connect', 'custom']
    output_name: str
    output_schema: Optional[AvroSchema]
    producer: Optional[SerializingProducer]
    producer_conf: Optional[Dict[str, Any]]
    consumer: Optional[DeserializingConsumer]
    schema_registry_client: Optional[SchemaRegistryClient]
    error_cb: Optional[Callable]
    delivery_cb: Optional[Callable]
    is_running: bool = False

    def __init__(
            self,
            name: str,
            writer: AvroSchema,
            producer: Optional[SerializingProducer] = None,
            schema_registry_client: Optional[SchemaRegistryClient] = None,
            admin_client: Optional[AdminClient] = None,
            reader: Optional[AvroSchema] = None,
            key_type: Optional[KeyType] = 'string',
            stats_cb: Callable = lambda stats: logger.info(stats),
            error_cb: Callable = lambda err: logger.error(err),
            delivery_cb: Callable = lambda msg, err: logger.info(msg) if err is None else logger.error(err),
            consumer_group_id: str = 'tradesignals-io',
            enable_auto_commit: bool = True,
            auto_commit_interval_ms: int = 100,
            auto_offset_reset: str = 'earliest'
        ):
        """Initialize the client with optional Avro schemas for input and output.
        """
        self.name = name
        self.writer = writer
        if self.writer is None:
            raise ValueError("Writer is required")
        self.key_type = key_type
        self.enable_auto_commit = enable_auto_commit
        self.auto_commit_interval_ms = auto_commit_interval_ms
        self.auto_offset_reset = auto_offset_reset
        self.stats_cb = stats_cb
        self.error_cb = error_cb
        self.delivery_cb = delivery_cb
        self.consumer_group_id = consumer_group_id
        self.reader = reader

        self.schema_registry_client = SchemaRegistryClient({
            'url': settings.schema_registry_url,
            'basic.auth.user.info': settings.schema_registry_auth_info
        })

        self.admin_client = AdminClient({
            'bootstrap.servers': settings.kafka_bootstrap_servers,
            'security.protocol': settings.kafka_security_protocol,
            'sasl.mechanism': settings.kafka_sasl_mechanism,
            'sasl.username': settings.kafka_sasl_username,
            'sasl.password': settings.kafka_sasl_password
        })

        self.consumer = None

        if self.reader is not None:
            self.consumer = DeserializingConsumer({
                'bootstrap.servers': settings.kafka_bootstrap_servers,
                'security.protocol': settings.kafka_security_protocol,
                'sasl.mechanism': settings.kafka_sasl_mechanism,
                'sasl.username': settings.kafka_sasl_username,
                'sasl.password': settings.kafka_sasl_password,
                'enable.auto.commit': enable_auto_commit,
                'auto.commit.interval.ms': auto_commit_interval_ms,
                'auto.offset.reset': auto_offset_reset,
                'group.id': consumer_group_id,
                'key.deserializer': (
                    StringDeserializer('utf_8') 
                    if self.key_type == 'string' 
                    else IntegerDeserializer() 
                    if self.key_type == 'integer' 
                    else DoubleDeserializer() 
                    if self.key_type == 'double' 
                    else None
                ),
                'value.deserializer': AvroDeserializer(
                    self.schema_registry_client,
                    self.reader().schema_str,
                    self.reader().from_dict
                )
            })

        self.producer = SerializingProducer({
            'bootstrap.servers': settings.kafka_bootstrap_servers,
            'security.protocol': settings.kafka_security_protocol,
            'sasl.mechanism': settings.kafka_sasl_mechanism,
            'sasl.username': settings.kafka_sasl_username,
            'sasl.password': settings.kafka_sasl_password,
            'statistics.interval.ms': 10 * 1000,  # 10 seconds
            'stats_cb': stats_cb,
            'error_cb': error_cb,
            'key.serializer': (
                StringSerializer('utf_8') 
                if self.key_type == 'string' 
                else IntegerSerializer() 
                if self.key_type == 'integer' 
                else DoubleSerializer() 
                if self.key_type == 'double' 
                else None
            ),
            'value.serializer': AvroSerializer(
                self.schema_registry_client,
                self.writer().schema_str,
                self.writer().to_dict
            )
        })
