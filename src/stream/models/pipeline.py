import typing as t
from abc import ABC, abstractmethod
from src.stream.models.schema import AvroSchema
from src.stream.config.settings import settings
from confluent_kafka import SerializingProducer, DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer, StringDeserializer
import logging

class Pipeline(ABC):
    """Base pipeline class for Kafka stream processing.

    Provides lazy initialization of Kafka producers and consumers.

    Args:
        name (str): Name of the pipeline.
        output_model (AvroSchema): Output schema model.
        input_model (Optional[AvroSchema]): Input schema model.

    Attributes:
        name (str): Pipeline name.
        _is_running (bool): Pipeline running status.
        _producer (SerializingProducer): Kafka producer.
        _consumer (DeserializingConsumer): Kafka consumer.
        _schema_registry (SchemaRegistryClient): Schema registry client.

    Example:
        ```python
        pipeline = Pipeline('example', OutputModel, InputModel)
        pipeline.run()
        ```
    """

    def __init__(
            self,
            name: str,
            output_model: t.Type[AvroSchema],
            input_model: t.Optional[t.Type[AvroSchema]] = None):
        """Initialize pipeline with configuration."""
        self.name = name
        if settings.log_level == 'INFO':
            self.logger = logging.getLogger(self.__class__.__name__)
        else:
            self.logger = logging.getLogger(self.__class__.__name__)
            self.logger.setLevel(logging.DEBUG)
        self._producer = None
        self._consumer = None
        self._schema_registry = None
        
        self.is_running = False
        self.output_model = output_model
        self.input_model = input_model

    def _init_schema_registry(self) -> SchemaRegistryClient:
        """Lazily initialize schema registry client.

        Returns:
            SchemaRegistryClient: Configured schema registry client.
        """
        if self._schema_registry is None:
            self._schema_registry = SchemaRegistryClient({
                'url': settings.schema_registry_url,
                'basic.auth.user.info': settings.schema_registry_auth_info
            })
        return self._schema_registry

    def _init_producer(self) -> SerializingProducer:
        """Lazily initialize Kafka producer.

        Returns:
            SerializingProducer: Configured Kafka producer.
        """
        if self._producer is None:
            producer_config = {
                'bootstrap.servers': settings.kafka_bootstrap_servers,
                'sasl.mechanisms': settings.kafka_sasl_mechanism,
                'sasl.username': settings.kafka_sasl_username,
                'sasl.password': settings.kafka_sasl_password,
                'security.protocol': settings.kafka_security_protocol,
                'acks': 'all',
                'batch.size': 1000
            }
            self._producer = SerializingProducer({
                **producer_config,
                'key.serializer': StringSerializer(),
                'value.serializer': StringSerializer()
            })
        return self._producer

    def _init_consumer(self) -> t.Optional[DeserializingConsumer]:
        """Lazily initialize Kafka consumer.

        Returns:
            Optional[DeserializingConsumer]: Configured Kafka consumer or None.
        """
        if self._consumer is None:
            consumer_config = {
                'bootstrap.servers': settings.kafka_bootstrap_servers,
                'group.id': settings.kafka_default_group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': 'false',
                'sasl.mechanisms': settings.kafka_sasl_mechanism,
                'sasl.username': settings.kafka_sasl_username,
                'sasl.password': settings.kafka_sasl_password,
                'security.protocol': settings.kafka_security_protocol
            }
            self._consumer = DeserializingConsumer({
                **consumer_config,
                'key.deserializer': StringDeserializer(),
                'value.deserializer': StringDeserializer()
            })
        return self._consumer
    
    @property
    def producer(self) -> SerializingProducer:
        """The producer for the pipeline."""
        return self._init_producer()
    
    @property
    def schema_registry(self) -> SchemaRegistryClient:
        """The schema registry client for the pipeline."""
        return self._init_schema_registry()
    
    @property
    def consumer(self) -> DeserializingConsumer:
        """The consumer for the pipeline."""
        return self._init_consumer()

    @abstractmethod
    def on_delivery(self, err, msg):
        """Report the delivery of a message.

        Args:
            err (str): The error message.
            msg (str): The message.
        """
        if err is not None:
            self.logger.error(f"Delivery failed for Msg: {msg.key()} : {err}")
        else:
            self.logger.info(
                f"Msg: {msg.key()} produced: {msg.topic()} "
                f"[{msg.partition()}] @ {msg.offset()}"
            )

    @abstractmethod
    def run(self):
        """Run the pipeline."""
        raise NotImplementedError("Subclasses must implement this method.")

    def start(self):
        """Start the pipeline."""
        self.is_running = True
        self.run()

    def stop(self):
        """Stop the pipeline."""
        self.is_running = False
        if self.producer is not None:
            self.producer.flush()
        if self.consumer is not None:
            self.consumer.close()
