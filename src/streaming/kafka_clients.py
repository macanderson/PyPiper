"""Kafka clients and settings.

Summary:
    Kafka clients and settings.

Attributes:
    KafkaSettings: Configuration settings for Kafka.
    KafkaProducer: Producer for Kafka using settings for authentication.
    KafkaConsumer: Consumer for Kafka using settings for authentication.
    KafkaAdminClient: Admin client for Kafka using settings for authentication.
    SchemaRegistryClient: Client for managing schemas in Kafka's Schema Registry.
"""
import attrs
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka import Producer, Consumer, KafkaException
from dotenv_derive import dotenv
from src.streaming.kafka_data import streaming_data


@dotenv(filename='.env')
class KafkaSettings:
    """Configuration settings for Kafka.

    Summary:
        Configuration settings for Kafka.

    Attributes:
        bootstrap_servers: str
        schema_registry_url: str
        schema_registry_auth_info: str
        security_protocol: str
        kafka_rest_url: str
        sasl_mechanism: str
        sasl_username: str
        sasl_password: str
        flink_env_id: str
        flink_job_id: str
        flink_org_id: str
        flink_cluster_id: str
        cloud_provider: str
        cloud_region: str

@attrs.define(frozen=True)
class KafkaProducer:
    """Producer for Kafka using settings for authentication.
    
    Summary:
        Producer for Kafka using settings for authentication.

    Attributes:
        producer: Producer

    Methods:
        produce(self, streaming_model: streaming_data) -> None:
            Produce messages to Kafka using model settings.
    """
    producer: Producer = attrs.field(factory=lambda: Producer({
        'bootstrap.servers': KafkaSettings.bootstrap_servers,
        'security.protocol': KafkaSettings.security_protocol,
        'sasl.mechanism': KafkaSettings.sasl_mechanism,
        'sasl.username': KafkaSettings.sasl_username,
        'sasl.password': KafkaSettings.sasl_password
    }))

@attrs.define(frozen=True)
class KafkaConsumer:
    """Consumer for Kafka using settings for authentication.
    
    Summary:
        Consumer for Kafka using settings for authentication.

    Attributes:
        consumer: Consumer

    Methods:
        consume(self, streaming_model: streaming_data) -> None:
            Consume messages from Kafka using model settings.
    """
    consumer: Consumer = attrs.field(factory=lambda: Consumer({
        'bootstrap.servers': KafkaSettings.bootstrap_servers,
        'security.protocol': KafkaSettings.security_protocol,
        'sasl.mechanism': KafkaSettings.sasl_mechanism,
        'sasl.username': KafkaSettings.sasl_username,
        'sasl.password': KafkaSettings.sasl_password,
        'group.id': 'example_group'
    }))

@attrs.define(frozen=True)
class KafkaAdminClient:
    """Admin client for Kafka using settings for authentication.
    
    Summary:
        Admin client for Kafka using settings for authentication.

    Attributes:
        admin_client: AdminClient

    Methods:
        create_topic(self, streaming_model: streaming_data) -> None:
            Create a Kafka topic using model settings.
    """
    admin_client: AdminClient = attrs.field(factory=lambda: AdminClient({
        'bootstrap.servers': KafkaSettings.bootstrap_servers,
        'security.protocol': KafkaSettings.security_protocol,
        'sasl.mechanism': KafkaSettings.sasl_mechanism,
        'sasl.username': KafkaSettings.sasl_username,
        'sasl.password': KafkaSettings.sasl_password
    }))

    def create_topic(self, streaming_model: streaming_data) -> None:
        """Create a Kafka topic using model settings.
        
        Summary:
            Create a Kafka topic using model settings.

        Args:
            streaming_model (streaming_data): Streaming model settings.
        """
        topic_config = {
            'topic': streaming_model.StreamingSettings.kafka_topic,
            'num_partitions': streaming_model.StreamingSettings.kafka_num_partitions,
            'replication_factor': streaming_model.StreamingSettings.kafka_replication_factor,
            'config': {
                'min.insync.replicas': streaming_model.StreamingSettings.kafka_min_insync_replicas,
                'cleanup.policy': streaming_model.StreamingSettings.kafka_cleanup_policy,
                'retention.ms': streaming_model.StreamingSettings.kafka_retention_ms,
                'retention.bytes': streaming_model.StreamingSettings.kafka_retention_bytes
            }
        }
        new_topic = NewTopic(**topic_config)
        self.admin_client.create_topics([new_topic])

    def create_kafka_topic(self, broker: str, topic_name: str, num_partitions: int = 1, replication_factor: int = 1):
        """Create a Kafka topic using Kafka Admin Client.
        
        Summary:
            Create a Kafka topic using Kafka Admin Client.

        Args:
            broker (str): Kafka broker address (e.g., "localhost:9092").
            topic_name (str): Name of the topic to create.
            num_partitions (int): Number of partitions for the topic.
            replication_factor (int): Replication factor for the topic.
        
        Raises:
            KafkaException: If topic creation fails.
        """
        admin_client = AdminClient({"bootstrap.servers": broker})
        new_topic = NewTopic(topic=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        try:
            fs = admin_client.create_topics([new_topic])
            for topic, f in fs.items():
                f.result()  # Trigger exception if failed
                print(f"Topic '{topic}' created successfully.")
        except KafkaException as e:
            print(f"Failed to create topic '{topic_name}': {e}")
        except Exception as ex:
            print(f"Unexpected error while creating topic '{topic_name}': {ex}")

@attrs.define(frozen=True)
class SchemaRegistryClient:
    """Client for managing schemas in Kafka's Schema Registry.
    
    Summary:
        Client for managing schemas in Kafka's Schema Registry.

    Attributes:
        schema_registry_client: SchemaRegistryClient

    Methods:
        register_schema(self, streaming_model: streaming_data) -> None:
            Register schema using model settings.
    """
    schema_registry_client: SchemaRegistryClient = attrs.field(factory=lambda: SchemaRegistryClient({
        'url': KafkaSettings.schema_registry_url,
        'auth': KafkaSettings.schema_registry_auth_info
    }))

    def register_schema(self, streaming_model: streaming_data) -> None:
        """Register schema using model settings."""
        schema = streaming_model.StreamingModelSettings.avro_schema
        schema_subject = f"{streaming_model.StreamingSettings.kafka_topic}-value"
        self.schema_registry_client.register(schema_subject, schema)
