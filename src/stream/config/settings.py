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
    environment (str): Environment of the application.
    log_level (str): Log level of the application.
    log_format (str): Log format of the application.
    github_pat (str): GitHub personal access token.
    pypi_token (str): PyPI token.
    pypi_username (str): PyPI username.
    package_name (str): Name of the package.


Example:
    settings = KafkaSettings()
    producer = Producer({'bootstrap.servers': settings.bootstrap_servers})
    print(settings.bootstrap_servers)
"""

from dotenv_derive import dotenv

@dotenv(dotenv_file='.env', traverse=True, coerce_values=True, extras='ignore')
class Settings:
    # schema registry settings
    schema_registry_url: str
    schema_registry_auth_info: str
    
    # kafka broker settings
    kafka_bootstrap_servers: str
    kafka_security_protocol: str
    kafka_rest_endpoint: str
    kafka_sasl_mechanism: str
    kafka_sasl_username: str
    kafka_sasl_password: str
    
    # new topic defaults
    kafka_default_group_id: str
    kafka_default_num_of_partitions: int
    kafka_default_replication_factor: int

    # message field serialization types
    kafka_default_key_type: str
    kafka_default_value_type: str

    # flink configurations
    flink_env_id: str
    flink_org_id: str
    flink_compute_pool_id: str
    flink_api_key: str
    flink_api_secret: str
    
    # default cloud infrastructure provider
    cloud_provider: str
    cloud_region: str
    
    # clickhouse database
    clickhouse_interface: str
    clickhouse_database: str
    clickhouse_host: str
    clickhouse_port: str
    clickhouse_user: str
    clickhouse_password: str

    # additional settings
    environment: str
    log_level: str
    log_format: str

    github_pat: str
    pypi_token: str
    pypi_username: str
    package_name: str

__all__ = [
    'Settings'
]

settings = Settings()

print(settings.environment)
print(settings.log_level)
print(settings.log_format)
