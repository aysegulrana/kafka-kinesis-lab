#!/usr/bin/env python
"""
Example of bridging data from Apache Kafka to Amazon Kinesis with enhanced logging.

This script demonstrates:
1. Consuming data from a Kafka topic
2. Processing the data (optionally)
3. Forwarding it to a Kinesis data stream

Usage:
- python kafka_to_kinesis.py
- python kafka_to_kinesis.py --aws (to use real AWS - may incur charges)

By default, uses LocalStack for Kinesis to avoid AWS charges.
"""

import json
import time
import boto3
import argparse
import os
import logging
from kafka import KafkaConsumer

from config import get_kafka_config, get_kinesis_config, get_bridge_config

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO  # You can change to DEBUG for even more detailed logs
)
logger = logging.getLogger(__name__)

def create_kinesis_client():
    """Create a Kinesis client using either LocalStack or real AWS."""
    config = get_kinesis_config()
    logger.info("Loading Kinesis configuration.")
    
    client_kwargs = {
        'region_name': config['region_name'],
    }
    
    # If using LocalStack, add endpoint URL and dummy credentials
    if config['use_localstack']:
        logger.info("Using LocalStack for AWS services (no AWS charges)")
        client_kwargs.update({
            'endpoint_url': config['endpoint_url'],
            'aws_access_key_id': 'test',
            'aws_secret_access_key': 'test',
        })
    else:
        logger.warning("WARNING: Using real AWS services - charges may apply")
        client_kwargs.update({
            'aws_access_key_id': config['aws_access_key_id'],
            'aws_secret_access_key': config['aws_secret_access_key'],
        })
    
    logger.debug("Creating Kinesis client with parameters: %s", client_kwargs)
    return boto3.client('kinesis', **client_kwargs)

def ensure_stream_exists(client, stream_name, shard_count):
    """Create the stream if it doesn't exist."""
    try:
        logger.info("Checking if stream '%s' exists...", stream_name)
        client.describe_stream(StreamName=stream_name)
        logger.info("Stream '%s' exists", stream_name)
    except client.exceptions.ResourceNotFoundException:
        logger.info("Stream '%s' not found. Creating stream...", stream_name)
        client.create_stream(
            StreamName=stream_name,
            ShardCount=shard_count
        )
        logger.info("Waiting for stream '%s' to become active...", stream_name)
        waiter = client.get_waiter('stream_exists')
        waiter.wait(StreamName=stream_name)
        logger.info("Stream '%s' is now active", stream_name)
    except Exception as e:
        logger.error("Error checking/creating stream: %s", e)
        logger.error("If using LocalStack, make sure it's running.")

def push_to_kinesis(record, kinesis_client, stream_name):
    """
    Sends a JSON record to the specified Kinesis stream.
    
    Args:
        record (dict): The record to send
        kinesis_client (boto3.client): Kinesis client
        stream_name (str): Name of the Kinesis stream
        
    Returns:
        dict: Response from Kinesis put_record call
    """
    logger.debug("Pushing record to Kinesis: %s", record)
    response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(record),
        PartitionKey=str(record.get('id', 'default'))  # Use record ID as partition key if available
    )
    logger.debug("Kinesis response: %s", response)
    return response

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Kafka to Kinesis Bridge')
    parser.add_argument('--aws', action='store_true', 
                        help='Use real AWS (may incur charges) instead of LocalStack')
    args = parser.parse_args()
    
    # Set environment variable based on command line argument
    if args.aws:
        os.environ['USE_LOCALSTACK'] = 'False'
        logger.warning("Using real AWS services - charges may apply")
    else:
        os.environ['USE_LOCALSTACK'] = 'True'
        logger.info("Using LocalStack for AWS services (no AWS charges)")
    
    logger.info("====== KAFKA TO KINESIS BRIDGE ======")
    
    # Load configurations
    logger.info("Loading configurations...")
    kafka_config = get_kafka_config()
    kinesis_config = get_kinesis_config()
    bridge_config = get_bridge_config()
    
    # Create a boto3 Kinesis client
    try:
        kinesis_client = create_kinesis_client()
        # Check if the Kinesis stream exists
        ensure_stream_exists(kinesis_client, bridge_config['kinesis_stream'], kinesis_config['shard_count'])
    except Exception as e:
        logger.error("Error setting up Kinesis client: %s", e)
        logger.error("Check your configuration or ensure LocalStack is running.")
        return
    
    # Create a Kafka consumer
    try:
        logger.info("Connecting to Kafka topic: %s", bridge_config['kafka_topic'])
        consumer = KafkaConsumer(
            bridge_config['kafka_topic'],
            bootstrap_servers=kafka_config['bootstrap_servers'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='kafka-kinesis-bridge-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        logger.info("Connected to Kafka topic: %s", bridge_config['kafka_topic'])
    except Exception as e:
        logger.error("Error connecting to Kafka: %s", e)
        logger.error("Check if Kafka is running and accessible.")
        return
    
    logger.info("Bridge is now running: Kafka (%s) → Kinesis (%s)", 
                bridge_config['kafka_topic'], bridge_config['kinesis_stream'])
    logger.info("Press Ctrl+C to stop...")
    
    # Record counters
    records_processed = 0
    records_failed = 0
    start_time = time.time()
    
    try:
        for message in consumer:
            logger.info("Received message from Kafka: %s", message)
            record = message.value
            
            # Optional: Process or transform the record here
            record['bridge_timestamp'] = time.time()
            logger.info("Processed record: %s", record)
            
            try:
                # Send to Kinesis
                response = push_to_kinesis(record, kinesis_client, bridge_config['kinesis_stream'])
                records_processed += 1
                logger.info("Record processed: %s", record.get('id', 'unknown'))
                
                # Log summary every 10 records
                if records_processed % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = records_processed / elapsed if elapsed > 0 else 0
                    logger.info("Processed %d records (%.2f records/sec)", records_processed, rate)
                    logger.info("Last record: %s → Shard: %s", record.get('id', 'unknown'), response['ShardId'])
            
            except Exception as e:
                records_failed += 1
                logger.error("Failed to send record to Kinesis: %s", e)
    
    except KeyboardInterrupt:
        logger.info("Bridge stopped by user")
    
    except Exception as e:
        logger.error("Unexpected error: %s", e)
    
    finally:
        if 'consumer' in locals():
            logger.info("Closing Kafka consumer...")
            consumer.close()
        
        # Print summary
        elapsed = time.time() - start_time
        rate = records_processed / elapsed if elapsed > 0 else 0
        
        logger.info("====== Bridge summary ======")
        logger.info("Total records processed: %d", records_processed)
        logger.info("Failed records: %d", records_failed)
        logger.info("Running time: %.2f seconds", elapsed)
        logger.info("Processing rate: %.2f records/second", rate)

if __name__ == '__main__':
    main()
