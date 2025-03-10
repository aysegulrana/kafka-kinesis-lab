#!/usr/bin/env python
"""
Example of bridging data from Apache Kafka to Amazon Kinesis.

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
from kafka import KafkaConsumer

from config import get_kafka_config, get_kinesis_config, get_bridge_config

def create_kinesis_client():
    """Create a Kinesis client using either LocalStack or real AWS."""
    config = get_kinesis_config()
    
    client_kwargs = {
        'region_name': config['region_name'],
    }
    
    # If using LocalStack, add endpoint URL and dummy credentials
    if config['use_localstack']:
        print("Using LocalStack for AWS services (no AWS charges)")
        client_kwargs.update({
            'endpoint_url': config['endpoint_url'],
            'aws_access_key_id': 'test',
            'aws_secret_access_key': 'test',
        })
    else:
        print("WARNING: Using real AWS services - charges may apply")
        client_kwargs.update({
            'aws_access_key_id': config['aws_access_key_id'],
            'aws_secret_access_key': config['aws_secret_access_key'],
        })
    
    return boto3.client('kinesis', **client_kwargs)

def ensure_stream_exists(client, stream_name, shard_count):
    """Create the stream if it doesn't exist."""
    try:
        client.describe_stream(StreamName=stream_name)
        print(f"Stream '{stream_name}' exists")
    except client.exceptions.ResourceNotFoundException:
        print(f"Creating stream '{stream_name}'...")
        client.create_stream(
            StreamName=stream_name,
            ShardCount=shard_count
        )
        # Wait for the stream to become active
        print(f"Waiting for stream '{stream_name}' to become active...")
        waiter = client.get_waiter('stream_exists')
        waiter.wait(StreamName=stream_name)
        print(f"Stream '{stream_name}' is now active")
    except Exception as e:
        print(f"Error checking/creating stream: {e}")
        print("If using LocalStack, make sure it's running.")

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
    response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(record),
        PartitionKey=str(record.get('id', 'default'))  # Use record ID as partition key if available
    )
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
        print("WARNING: Using real AWS services - charges may apply")
    else:
        os.environ['USE_LOCALSTACK'] = 'True'
    
    print("\n====== KAFKA TO KINESIS BRIDGE ======")
    
    # Load configurations
    kafka_config = get_kafka_config()
    kinesis_config = get_kinesis_config()
    bridge_config = get_bridge_config()
    
    # Create a boto3 Kinesis client
    try:
        kinesis_client = create_kinesis_client()
        
        # Check if the Kinesis stream exists
        ensure_stream_exists(kinesis_client, bridge_config['kinesis_stream'], kinesis_config['shard_count'])
    
    except Exception as e:
        print(f"Error setting up Kinesis client: {e}")
        print("Check your configuration or ensure LocalStack is running.")
        return
    
    # Create a Kafka consumer
    try:
        consumer = KafkaConsumer(
            bridge_config['kafka_topic'],
            bootstrap_servers=kafka_config['bootstrap_servers'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='kafka-kinesis-bridge-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print(f"Connected to Kafka topic: {bridge_config['kafka_topic']}")
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        print("Check if Kafka is running and accessible.")
        return
    
    print(f"\nBridge is now running: Kafka ({bridge_config['kafka_topic']}) → Kinesis ({bridge_config['kinesis_stream']})")
    print("Press Ctrl+C to stop...\n")
    
    # Record counters
    records_processed = 0
    records_failed = 0
    start_time = time.time()
    
    try:
        for message in consumer:
            record = message.value
            
            # Optional: Process or transform the record here
            # For example, add a timestamp or additional metadata
            record['bridge_timestamp'] = time.time()
            
            try:
                # Send to Kinesis
                response = push_to_kinesis(record, kinesis_client, bridge_config['kinesis_stream'])
                records_processed += 1
                
                if records_processed % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = records_processed / elapsed if elapsed > 0 else 0
                    print(f"Processed {records_processed} records ({rate:.2f} records/sec)")
                    print(f"Last record: {record.get('id', 'unknown')} → Shard: {response['ShardId']}")
            
            except Exception as e:
                records_failed += 1
                print(f"Failed to send record to Kinesis: {e}")
    
    except KeyboardInterrupt:
        print("\nBridge stopped by user")
    
    except Exception as e:
        print(f"\nUnexpected error: {e}")
    
    finally:
        if 'consumer' in locals():
            consumer.close()
        
        # Print summary
        elapsed = time.time() - start_time
        rate = records_processed / elapsed if elapsed > 0 else 0
        
        print("\nBridge summary:")
        print(f"- Total records processed: {records_processed}")
        print(f"- Failed records: {records_failed}")
        print(f"- Running time: {elapsed:.2f} seconds")
        print(f"- Processing rate: {rate:.2f} records/second")

if __name__ == '__main__':
    main()