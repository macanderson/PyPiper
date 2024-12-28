"""Configuration settings for Kafka and Flink."""
import pprint
from dotenv_derive import dotenv

@dotenv(dotenv_file='.env', traverse=True, coerce_values=True, extras='ignore')
class Settings:
    """Configuration settings for Kafka and Flink.

    This class holds the necessary settings for connecting to Kafka and 
    Flink services, including authentication and connection details.

    Attributes:
        schema_registry_url (str): URL for the schema registry service.
        schema_registry_auth (str): Authentication details for the schema registry.
        kafka_bootstrap_servers (str): Kafka bootstrap server addresses.
        kafka_security_protocol (str): Protocol used for Kafka security.
        kafka_rest_endpoint (str): REST endpoint for Kafka services.
        kafka_sasl_mechanism (str): SASL mechanism used for authentication.
        kafka_sasl_username (str): Username for SASL authentication.
        kafka_sasl_password (str): Password for SASL authentication.
        flink_env_id (str): Flink environment identifier.
        flink_org_id (str): Flink organization identifier.
        flink_compute_pool_id (str): Flink compute pool identifier.
        flink_api_key (str): Flink API key.
        flink_api_secret (str): Flink API secret.
        cloud_provider (str): Provider of cloud services (e.g., AWS).
        cloud_region (str): Region of the cloud service (e.g., us-west-2).
        clickhouse_interface (str): Clickhouse interface.
        clickhouse_database (str): Clickhouse database.
        clickhouse_host (str): Clickhouse host.
        clickhouse_port (str): Clickhouse port.
        clickhouse_user (str): Clickhouse user.
        clickhouse_password (str): Clickhouse password.

    Example:
        settings = KafkaSettings()
        print(settings.bootstrap_servers)
    """
    schema_registry_url: str
    schema_registry_auth_info: str
    kafka_bootstrap_servers: str
    kafka_security_protocol: str
    kafka_rest_endpoint: str
    kafka_sasl_mechanism: str
    kafka_sasl_username: str
    kafka_sasl_password: str
    flink_env_id: str
    flink_org_id: str
    flink_compute_pool_id: str
    flink_api_key: str
    flink_api_secret: str
    cloud_provider: str
    cloud_region: str
    clickhouse_interface: str
    clickhouse_database: str
    clickhouse_host: str
    clickhouse_port: str
    clickhouse_user: str
    clickhouse_password: str

__all__ = [
    'Settings'
]
