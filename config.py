"""
Configuration settings for Kafka and Kinesis.
"""

import os

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'topic': 'data-stream',
    'group_id': 'data-processing-group',
    'client_id': 'kafka-python-client',
    'analytics_topic': 'analytics'
}

# Kinesis Configuration
# Reduces resource usage to fit within Free Tier limits
KINESIS_CONFIG = {
    'stream_name': 'data-stream',
    'region_name': os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'),
    'aws_access_key_id': os.environ.get('AWS_ACCESS_KEY_ID', 'YOUR_ACCESS_KEY'),
    'aws_secret_access_key': os.environ.get('AWS_SECRET_ACCESS_KEY', 'YOUR_SECRET_KEY'),
    'shard_count': 1,  # Reduced to 1 shard to stay within Free Tier
    'analytics_stream': 'analytics',
    # Set to True to use local mock instead of actual AWS Kinesis
    'use_localstack': os.environ.get('USE_LOCALSTACK', 'True').lower() in ('true', 't', '1', 'yes', 'y')
}

# LocalStack endpoint (if using mock AWS services)
if KINESIS_CONFIG['use_localstack']:
    KINESIS_CONFIG['endpoint_url'] = 'http://localhost:4566'  # LocalStack default endpoint

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