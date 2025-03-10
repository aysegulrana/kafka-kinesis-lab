#!/usr/bin/env python
"""
Performance testing examples for Kafka and Kinesis.

This script runs performance tests to compare Kafka and Kinesis throughput and latency.

Run with:
- python performance_tests.py
- python performance_tests.py --aws (to use real AWS - may incur charges)

By default, uses LocalStack for Kinesis to avoid AWS charges.
"""

import time
import statistics
import json
import uuid
import random
import argparse
import os
from concurrent.futures import ThreadPoolExecutor
from kafka import KafkaProducer, KafkaConsumer
import boto3

from config import get_kafka_config, get_kinesis_config

# --- HELPER FUNCTIONS ---
def create_kinesis_client():
    """Create a Kinesis client using either LocalStack or real AWS."""
    config = get_kinesis_config()
    
    client_kwargs = {
        'region_name': config['region_name'],
    }
    
    # If using LocalStack, add endpoint URL and dummy credentials
    print("Using LocalStack for AWS services (no AWS charges)")
    client_kwargs.update({
        'endpoint_url': config['endpoint_url'],
        'aws_access_key_id': 'test',
        'aws_secret_access_key': 'test',
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

# Sample data generator
def generate_sample_data(size_bytes=1024):
    """Generate random data of approximately the specified size."""
    base_data = {
        'id': str(uuid.uuid4()),
        'timestamp': time.time(),
        'category': random.choice(['A', 'B', 'C', 'D', 'E']),
        'values': []
    }
    
    # Fill with random values to reach target size
    current_size = len(json.dumps(base_data))
    while current_size < size_bytes:
        base_data['values'].append(random.random())
        current_size = len(json.dumps(base_data))
        
    return base_data

# ===== KAFKA PERFORMANCE TESTING =====
def test_kafka_producer_performance(bootstrap_servers, topic, num_messages, message_size):
    """Test Kafka producer performance."""
    print(f"Testing Kafka producer performance with {num_messages} messages...")
    
    # Create Kafka producer
    producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # Robust configuration
            acks='all',             # Wait for all replicas to acknowledge
            retries=3,               # Number of retry attempts
            retry_backoff_ms=1000,   # Wait between retries
            request_timeout_ms=30000,# Longer timeout
            linger_ms=5,             # Small delay to batch messages
            batch_size=16384,        # Batch size in bytes
            compression_type='gzip'  # Compress messages
        )
    
    latencies = []
    start_time = time.time()
    
    try:
        for i in range(num_messages):
            data = generate_sample_data(message_size)
            partition_key = data['category'].encode('utf-8')
            
            # Measure latency for each message
            msg_start = time.time()
            future = producer.send(topic, key=partition_key, value=data)
            future.get(timeout=10)  # Wait for send to complete
            latency = time.time() - msg_start
            latencies.append(latency * 1000)  # Convert to ms
            
            # Progress indicator
            if (i + 1) % 100 == 0 or i == 0:
                print(f"  Sent {i + 1}/{num_messages} messages")
    
    finally:
        producer.flush()
        producer.close()
    
    total_time = time.time() - start_time
    throughput = num_messages / total_time if total_time > 0 else 0
    
    if not latencies:
        return {
            'throughput_msgs_per_sec': 0,
            'avg_latency_ms': 0,
            'p95_latency_ms': 0,
            'p99_latency_ms': 0,
            'total_time_sec': total_time
        }
    
    return {
        'throughput_msgs_per_sec': throughput,
        'avg_latency_ms': statistics.mean(latencies),
        'p95_latency_ms': sorted(latencies)[int(len(latencies) * 0.95)],
        'p99_latency_ms': sorted(latencies)[int(len(latencies) * 0.99)],
        'total_time_sec': total_time
    }

def test_kafka_consumer_performance(bootstrap_servers, topic, group_id, max_messages):
    """Test Kafka consumer performance."""
    print(f"Testing Kafka consumer performance with up to {max_messages} messages...")
    
    # Create Kafka consumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        max_poll_records=500,  # Large batch size for better performance
        fetch_max_wait_ms=500
    )
    
    start_time = time.time()
    message_count = 0
    processing_times = []
    
    try:
        for message in consumer:
            msg_start = time.time()
            # Simple processing simulation
            _ = message.value
            processing_time = time.time() - msg_start
            processing_times.append(processing_time * 1000)  # Convert to ms
            
            message_count += 1
            
            # Progress indicator
            if message_count % 100 == 0 or message_count == 1:
                print(f"  Processed {message_count}/{max_messages} messages")
                
            if message_count >= max_messages:
                break
    
    finally:
        consumer.close()
    
    total_time = time.time() - start_time
    throughput = message_count / total_time if total_time > 0 else 0
    
    if not processing_times:
        return {
            'throughput_msgs_per_sec': 0,
            'avg_processing_ms': 0,
            'p95_processing_ms': 0,
            'total_time_sec': total_time
        }
    
    return {
        'throughput_msgs_per_sec': throughput,
        'avg_processing_ms': statistics.mean(processing_times),
        'p95_processing_ms': sorted(processing_times)[int(len(processing_times) * 0.95)],
        'total_time_sec': total_time,
        'messages_processed': message_count
    }

# ===== KINESIS PERFORMANCE TESTING =====
def test_kinesis_producer_performance(client, stream_name, num_messages, message_size, batch_size=25):
    """Test Kinesis producer performance."""
    print(f"Testing Kinesis producer performance with {num_messages} messages...")
    
    latencies = []
    start_time = time.time()
    
    try:
        # Single record mode
        if batch_size <= 1:
            for i in range(num_messages):
                data = generate_sample_data(message_size)
                partition_key = data['category']
                
                msg_start = time.time()
                response = client.put_record(
                    StreamName=stream_name,
                    Data=json.dumps(data),
                    PartitionKey=partition_key
                )
                latency = time.time() - msg_start
                latencies.append(latency * 1000)  # Convert to ms
                
                # Progress indicator
                if (i + 1) % 100 == 0 or i == 0:
                    print(f"  Sent {i + 1}/{num_messages} messages")
        
        # Batch mode
        else:
            for i in range(0, num_messages, batch_size):
                records = []
                batch_end = min(i + batch_size, num_messages)
                
                for j in range(i, batch_end):
                    data = generate_sample_data(message_size)
                    partition_key = data['category']
                    
                    records.append({
                        'Data': json.dumps(data),
                        'PartitionKey': partition_key
                    })
                
                msg_start = time.time()
                response = client.put_records(
                    StreamName=stream_name,
                    Records=records
                )
                latency = time.time() - msg_start
                
                # Record individual latencies for each record in the batch
                for j in range(len(records)):
                    latencies.append(latency * 1000 / len(records))
                
                # Progress indicator
                print(f"  Sent {min(i + batch_size, num_messages)}/{num_messages} messages")
    
    except Exception as e:
        print(f"Error during Kinesis producer test: {e}")
    
    total_time = time.time() - start_time
    throughput = num_messages / total_time if total_time > 0 else 0
    
    if not latencies:
        return {
            'throughput_msgs_per_sec': 0,
            'avg_latency_ms': 0,
            'p95_latency_ms': 0,
            'p99_latency_ms': 0,
            'total_time_sec': total_time
        }
    
    return {
        'throughput_msgs_per_sec': throughput,
        'avg_latency_ms': statistics.mean(latencies),
        'p95_latency_ms': sorted(latencies)[int(len(latencies) * 0.95)],
        'p99_latency_ms': sorted(latencies)[int(len(latencies) * 0.99)],
        'total_time_sec': total_time
    }

def test_kinesis_consumer_performance(client, stream_name, max_messages):
    """Test Kinesis consumer performance."""
    print(f"Testing Kinesis consumer performance with up to {max_messages} messages...")
    
    try:
        # Get shard information
        response = client.describe_stream(StreamName=stream_name)
        shards = response['StreamDescription']['Shards']
        
        message_count = 0
        processing_times = []
        start_time = time.time()
        
        # Process all shards in parallel
        def process_shard(shard_id):
            local_count = 0
            local_processing_times = []
            
            try:
                iterator_response = client.get_shard_iterator(
                    StreamName=stream_name,
                    ShardId=shard_id,
                    ShardIteratorType='TRIM_HORIZON'
                )
                
                shard_iterator = iterator_response['ShardIterator']
                max_per_shard = max_messages // len(shards) + 1  # Add 1 to handle rounding
                
                while shard_iterator and local_count < max_per_shard:
                    records_response = client.get_records(
                        ShardIterator=shard_iterator,
                        Limit=1000  # Maximum allowed
                    )
                    
                    for record in records_response['Records']:
                        msg_start = time.time()
                        # Simple processing simulation
                        _ = json.loads(record['Data'].decode('utf-8'))
                        processing_time = time.time() - msg_start
                        local_processing_times.append(processing_time * 1000)  # Convert to ms
                        
                        local_count += 1
                        
                        if local_count % 100 == 0:
                            print(f"  Shard {shard_id}: Processed {local_count}/{max_per_shard} messages")
                        
                        if local_count >= max_per_shard:
                            break
                    
                    if not records_response['Records']:
                        # If there are no records, we're at the end of the shard
                        break
                    
                    shard_iterator = records_response.get('NextShardIterator')
                    time.sleep(0.2)  # To avoid throttling
            
            except Exception as e:
                print(f"Error processing shard {shard_id}: {e}")
            
            return local_count, local_processing_times
        
        # Process shards in parallel
        with ThreadPoolExecutor(max_workers=len(shards)) as executor:
            results = list(executor.map(process_shard, [shard['ShardId'] for shard in shards]))
            
            for count, times in results:
                message_count += count
                processing_times.extend(times)
        
    except Exception as e:
        print(f"Error during Kinesis consumer test: {e}")
        return {
            'throughput_msgs_per_sec': 0,
            'avg_processing_ms': 0,
            'p95_processing_ms': 0,
            'total_time_sec': 0,
            'messages_processed': 0
        }
    
    total_time = time.time() - start_time
    throughput = message_count / total_time if total_time > 0 else 0
    
    if not processing_times:
        return {
            'throughput_msgs_per_sec': 0,
            'avg_processing_ms': 0,
            'p95_processing_ms': 0,
            'total_time_sec': total_time,
            'messages_processed': message_count
        }
    
    return {
        'throughput_msgs_per_sec': throughput,
        'avg_processing_ms': statistics.mean(processing_times),
        'p95_processing_ms': sorted(processing_times)[int(len(processing_times) * 0.95)] if len(processing_times) > 20 else 0,
        'total_time_sec': total_time,
        'messages_processed': message_count
    }

# ===== RUN PERFORMANCE TESTS =====
def run_performance_comparison(use_localstack=True):
    """Run a performance comparison between Kafka and Kinesis."""
    print("\n====== PERFORMANCE TESTING ======")
    print("Starting performance tests...")
    
    # Test parameters - smaller for the lab
    num_messages = 1000  # Reduced for lab purposes
    message_size = 1024  # 1KB
    
    # Topic and stream names
    kafka_topic = 'perf-test'
    kinesis_stream = 'perf-test'
    
    # Get configurations
    kafka_config = get_kafka_config()
    kinesis_config = get_kinesis_config()
    
    # Create Kinesis client using LocalStack
    kinesis_client = create_kinesis_client()
    
    # Create the Kinesis stream if it doesn't exist
    ensure_stream_exists(kinesis_client, kinesis_stream, kinesis_config['shard_count'])
    
    # === PRODUCER TESTS ===
    print("\n--- Starting Producer Performance Tests ---")
    
    # Kafka producer test
    kafka_producer_results = test_kafka_producer_performance(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        topic=kafka_topic,
        num_messages=num_messages,
        message_size=message_size
    )
    
    # Kinesis producer test
    kinesis_producer_results = test_kinesis_producer_performance(
        client=kinesis_client,
        stream_name=kinesis_stream,
        num_messages=num_messages,
        message_size=message_size,
        batch_size=25  # Batch for better performance
    )
    
    # === CONSUMER TESTS ===
    print("\n--- Starting Consumer Performance Tests ---")
    
    # Wait a bit to ensure all messages are available
    print("Waiting for messages to be available for consumers...")
    time.sleep(5)
    
    # Kafka consumer test
    kafka_consumer_results = test_kafka_consumer_performance(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        topic=kafka_topic,
        group_id='perf-test-group',
        max_messages=num_messages
    )
    
    # Kinesis consumer test
    kinesis_consumer_results = test_kinesis_consumer_performance(
        client=kinesis_client,
        stream_name=kinesis_stream,
        max_messages=num_messages
    )
    
    # === PRINT RESULTS ===
    print("\n===== PERFORMANCE RESULTS =====")
    print(f"Message Size: {message_size} bytes, Count: {num_messages}")
    
    print("\n--- PRODUCER RESULTS ---")
    print("\nKafka Producer:")
    for key, value in kafka_producer_results.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.2f}")
        else:
            print(f"  {key}: {value}")
            
    print("\nKinesis Producer:")
    for key, value in kinesis_producer_results.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.2f}")
        else:
            print(f"  {key}: {value}")
    
    print("\n--- CONSUMER RESULTS ---")
    print("\nKafka Consumer:")
    for key, value in kafka_consumer_results.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.2f}")
        else:
            print(f"  {key}: {value}")
            
    print("\nKinesis Consumer:")
    for key, value in kinesis_consumer_results.items():
        if isinstance(value, float):
            print(f"  {key}: {value:.2f}")
        else:
            print(f"  {key}: {value}")
    
    print("\n--- PERFORMANCE COMPARISON ---")
    
    # Producer comparison
    kp_throughput = kafka_producer_results['throughput_msgs_per_sec']
    kinp_throughput = kinesis_producer_results['throughput_msgs_per_sec']
    throughput_ratio = kp_throughput / kinp_throughput if kinp_throughput > 0 else float('inf')
    
    kp_latency = kafka_producer_results['avg_latency_ms']
    kinp_latency = kinesis_producer_results['avg_latency_ms']
    latency_ratio = kinp_latency / kp_latency if kp_latency > 0 else float('inf')
    
    print("\nProducer Performance Ratio (Kafka / Kinesis):")
    print(f"  Throughput: {throughput_ratio:.2f}x (higher means Kafka is faster)")
    print(f"  Latency: {latency_ratio:.2f}x (higher means Kinesis is slower)")
    
    # Consumer comparison
    kc_throughput = kafka_consumer_results['throughput_msgs_per_sec']
    kinc_throughput = kinesis_consumer_results['throughput_msgs_per_sec']
    c_throughput_ratio = kc_throughput / kinc_throughput if kinc_throughput > 0 else float('inf')
    
    kc_latency = kafka_consumer_results['avg_processing_ms']
    kinc_latency = kinesis_consumer_results['avg_processing_ms']
    c_latency_ratio = kinc_latency / kc_latency if kc_latency > 0 else float('inf')
    
    print("\nConsumer Performance Ratio (Kafka / Kinesis):")
    print(f"  Throughput: {c_throughput_ratio:.2f}x (higher means Kafka is faster)")
    print(f"  Latency: {c_latency_ratio:.2f}x (higher means Kinesis is slower)")
    
    # Summary note about LocalStack vs real AWS
    if kinesis_config['use_localstack']:
        print("\nNOTE: These tests used LocalStack to simulate Kinesis. Actual AWS Kinesis")
        print("performance may differ significantly in a production environment.")
    
    print("\nPerformance tests completed.")
    
    return {
        "kafka_producer": kafka_producer_results,
        "kinesis_producer": kinesis_producer_results,
        "kafka_consumer": kafka_consumer_results,
        "kinesis_consumer": kinesis_consumer_results
    }

def main():
    """Parse command line arguments and run performance tests."""
    parser = argparse.ArgumentParser(description='Performance testing for Kafka and Kinesis')
    parser.add_argument('--aws', action='store_true', 
                       help='Use real AWS (may incur charges) instead of LocalStack')
    
    args = parser.parse_args()
    
    # Set environment variable based on command line argument
    if args.aws:
        os.environ['USE_LOCALSTACK'] = 'False'
        print("WARNING: Using real AWS services - charges may apply")
    else:
        os.environ['USE_LOCALSTACK'] = 'True'
    
    # Run performance tests
    run_performance_comparison()

if __name__ == '__main__':
    main()