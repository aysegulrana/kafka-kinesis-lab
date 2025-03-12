"""
Configuration settings for Kafka and Kinesis.
"""

import os

KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:29092'],  # Use the container's internal IP
    'topic': 'data-stream',
    'group_id': 'data-processing-group', # Consumer group ID
    'client_id': 'kafka-python-client',
    'analytics_topic': 'analytics'
}

# Kinesis Configuration
# Set to use LocalStack by default to avoid AWS charges
KINESIS_CONFIG = {
    'stream_name': 'data-stream',
    'region_name': os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'),
    'aws_access_key_id': os.environ.get('AWS_ACCESS_KEY_ID', 'test'),
    'aws_secret_access_key': os.environ.get('AWS_SECRET_ACCESS_KEY', 'test'),
    'shard_count': 1,
    'analytics_stream': 'analytics',
    'use_localstack': True,
    'endpoint_url': 'http://localhost:4566'  # LocalStack endpoint
}

# Kafka to Kinesis Bridge Configuration
BRIDGE_CONFIG = {
    'kafka_topic': 'data-stream',
    'kinesis_stream': 'data-stream'
}

def get_kafka_config():
    """Returns the Kafka configuration settings."""
    return KAFKA_CONFIG

def get_kinesis_config():
    """Returns the Kinesis configuration settings."""
    return KINESIS_CONFIG

def get_bridge_config():
    """Returns the Kafka to Kinesis bridge configuration settings."""
    return BRIDGE_CONFIG
