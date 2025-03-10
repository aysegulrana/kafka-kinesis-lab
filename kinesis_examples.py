#!/usr/bin/env python
"""
Examples of using Amazon Kinesis for data streaming.
This file contains three main examples:
1. Kinesis Producer
2. Kinesis Consumer
3. Kinesis Real-time Analytics Pipeline

Run with:
- python kinesis_examples.py producer
- python kinesis_examples.py consumer
- python kinesis_examples.py analytics

By default, this uses LocalStack to avoid AWS charges.
To use real AWS Kinesis (may incur charges), set:
export USE_LOCALSTACK=False
"""

import argparse
import json
import time
import random
import uuid
import threading
from datetime import datetime
import boto3
import os

from config import get_kinesis_config

# --- KINESIS SETUP HELPER --- 
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

# Helper function to ensure a stream exists
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

# --- KINESIS PRODUCER ---
def kinesis_producer_example():
    """Example implementation of a Kinesis producer."""
    print("\n====== KINESIS PRODUCER EXAMPLE ======")
    
    # Get configuration
    config = get_kinesis_config()
    
    # Create a Kinesis client
    client = create_kinesis_client()
    
    # Ensure stream exists
    ensure_stream_exists(client, config['stream_name'], config['shard_count'])
    
    def generate_data():
        """Generate a sample data record."""
        return {
            'id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'value': random.randint(1, 100),
            'category': random.choice(['A', 'B', 'C']),
            'metadata': {
                'source': 'kinesis-producer',
                'version': '1.0'
            }
        }
    
    try:
        print("Sending messages to Kinesis stream:", config['stream_name'])
        
        # Individual record sends
        for i in range(5):  # Send 5 individual records
            data = generate_data()
            
            # In Kinesis, the partition key determines which shard receives the data
            partition_key = data['category']
            
            # Put the record to Kinesis
            response = client.put_record(
                StreamName=config['stream_name'],
                Data=json.dumps(data),
                PartitionKey=partition_key
            )
            
            print(f"Message {i+1} sent to shard {response['ShardId']} "
                  f"with sequence number {response['SequenceNumber']}")
            
            time.sleep(0.5)
        
        # Batch send
        print("\nSending a batch of 5 records...")
        batch_records = []
        
        for i in range(5):  # Prepare 5 records for batch sending
            data = generate_data()
            partition_key = data['category']
            
            batch_records.append({
                'Data': json.dumps(data),
                'PartitionKey': partition_key
            })
        
        # Send batch to Kinesis
        response = client.put_records(
            StreamName=config['stream_name'],
            Records=batch_records
        )
        
        # Check for failed records
        failed_count = response.get('FailedRecordCount', 0)
        if failed_count > 0:
            print(f"{failed_count} records failed to be processed")
            
        for i, record in enumerate(response.get('Records', [])):
            if 'ErrorCode' in record:
                print(f"Record {i+1} failed: {record['ErrorCode']} - {record['ErrorMessage']}")
            else:
                print(f"Record {i+1} sent to shard {record['ShardId']}")
    
    except Exception as e:
        print(f"Error in producer: {e}")
        if config['use_localstack']:
            print("If using LocalStack, check if it's running correctly.")
        else:
            print("If using AWS, check your AWS credentials and configuration.")
    
    finally:
        print("Producer operation completed.")

# --- KINESIS CONSUMER ---
def kinesis_consumer_example():
    """Example implementation of a Kinesis consumer."""
    print("\n====== KINESIS CONSUMER EXAMPLE ======")
    
    # Get configuration
    config = get_kinesis_config()
    
    # Create a Kinesis client
    client = create_kinesis_client()
    
    # Ensure stream exists
    ensure_stream_exists(client, config['stream_name'], config['shard_count'])
    
    try:
        print(f"Consuming messages from Kinesis stream: {config['stream_name']}")
        print("Press Ctrl+C to stop...")
        
        # Get information about the shards in the stream
        response = client.describe_stream(StreamName=config['stream_name'])
        shards = response['StreamDescription']['Shards']
        
        print(f"Stream has {len(shards)} shards")
        
        # For each shard, get a shard iterator and start reading data
        for shard in shards:
            shard_id = shard['ShardId']
            print(f"Reading from shard: {shard_id}")
            
            # Get a shard iterator
            iterator_response = client.get_shard_iterator(
                StreamName=config['stream_name'],
                ShardId=shard_id,
                ShardIteratorType='TRIM_HORIZON'  # Start from the oldest record
            )
            
            shard_iterator = iterator_response['ShardIterator']
            record_count = 0
            empty_responses = 0
            max_empty_responses = 3  # Stop after 3 empty responses to avoid hanging
            
            # Read records from the shard
            while shard_iterator and empty_responses < max_empty_responses:
                records_response = client.get_records(
                    ShardIterator=shard_iterator,
                    Limit=25  # Maximum number of records to return
                )
                
                # Process the records
                if records_response['Records']:
                    empty_responses = 0  # Reset counter when we get records
                    
                    for record in records_response['Records']:
                        record_count += 1
                        data = json.loads(record['Data'].decode('utf-8'))
                        
                        print(f"\nReceived record from shard {shard_id}")
                        print(f"Partition key: {record['PartitionKey']}")
                        print(f"Sequence number: {record['SequenceNumber']}")
                        print(f"Data: {data}")
                        print("---")
                else:
                    empty_responses += 1
                    print(f"No records available in shard {shard_id} ({empty_responses}/{max_empty_responses})")
                
                # Get the next shard iterator
                shard_iterator = records_response.get('NextShardIterator')
                
                # Add a small delay to avoid throttling
                time.sleep(1)
            
            print(f"Finished reading from shard {shard_id}, found {record_count} records")
    
    except KeyboardInterrupt:
        print("\nConsumer stopped by user")
    except Exception as e:
        print(f"Error in consumer: {e}")
        if config['use_localstack']:
            print("If using LocalStack, check if it's running correctly.")
        else:
            print("If using AWS, check your AWS credentials and configuration.")
    
    finally:
        print("Consumer operation completed.")

# --- KINESIS REAL-TIME ANALYTICS PIPELINE ---
def kinesis_analytics_pipeline():
    """
    Example of a real-time analytics pipeline using Kinesis.
    
    This example demonstrates:
    1. A data source generating events
    2. A processor analyzing events in real-time
    3. A consumer storing and displaying analytics results
    """
    print("\n====== KINESIS REAL-TIME ANALYTICS PIPELINE ======")
    
    # Get configuration
    config = get_kinesis_config()
    
    # Create a Kinesis client
    client = create_kinesis_client()
    
    # Check if streams exist, create them if they don't
    ensure_stream_exists(client, config['stream_name'], config['shard_count'])
    ensure_stream_exists(client, config['analytics_stream'], config['shard_count'])
    
    # Simulated data source for Kinesis
    class KinesisDataSource:
        def __init__(self, client, stream_name):
            self.client = client
            self.stream_name = stream_name
            self.running = False
            
        def generate_event(self):
            # Simulate a user activity event
            event = {
                'user_id': f"user_{random.randint(1, 1000)}",
                'event_type': random.choice(['view', 'click', 'purchase']),
                'product_id': f"product_{random.randint(1, 500)}",
                'category': random.choice(['electronics', 'clothing', 'home', 'books']),
                'price': round(random.uniform(5.99, 299.99), 2),
                'timestamp': datetime.now().isoformat()
            }
            return event
            
        def start(self, events_per_second=5):
            self.running = True
            
            def _run():
                while self.running:
                    try:
                        event = self.generate_event()
                        
                        # Send to Kinesis
                        self.client.put_record(
                            StreamName=self.stream_name,
                            Data=json.dumps(event),
                            PartitionKey=event['user_id']
                        )
                        
                        time.sleep(1 / events_per_second)
                    except Exception as e:
                        print(f"Data source error: {e}")
                        time.sleep(1)  # Brief pause before retrying
            
            self.thread = threading.Thread(target=_run)
            self.thread.start()
            print(f"Data source started - generating {events_per_second} events/second")
            
        def stop(self):
            self.running = False
            if hasattr(self, 'thread'):
                self.thread.join()
            print("Data source stopped")
    
    # Real-time processor using Kinesis
    class KinesisProcessor:
        def __init__(self, client, input_stream, output_stream):
            self.client = client
            self.input_stream = input_stream
            self.output_stream = output_stream
            self.running = False
            
            # Get shard information
            response = client.describe_stream(StreamName=input_stream)
            self.shards = response['StreamDescription']['Shards']
            
            # Analytics state
            self.category_counts = {}
            self.user_sessions = {}
            self.product_views = {}
            self.window_size = 60  # 60 second window
            
        def process_event(self, event):
            # Update category counts
            category = event['category']
            self.category_counts[category] = self.category_counts.get(category, 0) + 1
            
            # Track user sessions
            user_id = event['user_id']
            if user_id not in self.user_sessions:
                self.user_sessions[user_id] = {
                    'first_seen': event['timestamp'],
                    'events': []
                }
            self.user_sessions[user_id]['last_seen'] = event['timestamp']
            self.user_sessions[user_id]['events'].append(event['event_type'])
            
            # Track product views/purchases
            if event['event_type'] in ['view', 'purchase']:
                product_id = event['product_id']
                if product_id not in self.product_views:
                    self.product_views[product_id] = {'views': 0, 'purchases': 0}
                
                if event['event_type'] == 'view':
                    self.product_views[product_id]['views'] += 1
                else:
                    self.product_views[product_id]['purchases'] += 1
            
            # Generate analytics
            return self.generate_analytics()
            
        def generate_analytics(self):
            # Calculate conversion rates
            product_insights = []
            for product_id, stats in self.product_views.items():
                if stats['views'] > 0:
                    conversion_rate = (stats['purchases'] / stats['views']) * 100
                    product_insights.append({
                        'product_id': product_id,
                        'views': stats['views'],
                        'purchases': stats['purchases'],
                        'conversion_rate': round(conversion_rate, 2)
                    })
            
            # Sort by conversion rate
            product_insights.sort(key=lambda x: x['conversion_rate'], reverse=True)
            
            # Get top categories
            top_categories = sorted(
                [{'category': k, 'count': v} for k, v in self.category_counts.items()],
                key=lambda x: x['count'],
                reverse=True
            )[:5]
            
            # Active users count
            active_users = len(self.user_sessions)
            
            return {
                'timestamp': datetime.now().isoformat(),
                'window_size_seconds': self.window_size,
                'active_users': active_users,
                'top_categories': top_categories,
                'top_converting_products': product_insights[:5]
            }
            
        def process_shard(self, shard_id):
            # Get a shard iterator
            iterator_response = self.client.get_shard_iterator(
                StreamName=self.input_stream,
                ShardId=shard_id,
                ShardIteratorType='LATEST'
            )
            
            shard_iterator = iterator_response['ShardIterator']
            
            while self.running and shard_iterator:
                try:
                    response = self.client.get_records(
                        ShardIterator=shard_iterator,
                        Limit=100
                    )
                    
                    records = response['Records']
                    
                    for record in records:
                        event = json.loads(record['Data'].decode('utf-8'))
                        analytics = self.process_event(event)
                        
                        # Send analytics to output stream
                        self.client.put_record(
                            StreamName=self.output_stream,
                            Data=json.dumps(analytics),
                            PartitionKey='analytics'
                        )
                    
                    shard_iterator = response.get('NextShardIterator')
                    
                    # Avoid throttling
                    if not records:
                        time.sleep(1)
                    else:
                        time.sleep(0.1)
                except Exception as e:
                    print(f"Processor error in shard {shard_id}: {e}")
                    time.sleep(1)  # Brief pause before retrying
                    
                    # Try to get a new iterator
                    try:
                        iterator_response = self.client.get_shard_iterator(
                            StreamName=self.input_stream,
                            ShardId=shard_id,
                            ShardIteratorType='LATEST'
                        )
                        shard_iterator = iterator_response['ShardIterator']
                    except Exception:
                        # If we can't get a new iterator, break the loop
                        break
                    
        def start(self):
            self.running = True
            self.threads = []
            
            # Start a thread for each shard
            for shard in self.shards:
                thread = threading.Thread(
                    target=self.process_shard,
                    args=(shard['ShardId'],)
                )
                thread.start()
                self.threads.append(thread)
            
            print(f"Real-time processor started with {len(self.threads)} shard processors")
                
        def stop(self):
            self.running = False
            for thread in self.threads:
                thread.join()
            print("Processor stopped")
    
    # Analytics Consumer for Kinesis
    class KinesisAnalyticsConsumer:
        def __init__(self, client, stream_name):
            self.client = client
            self.stream_name = stream_name
            self.running = False
            
            # Get shard information
            response = client.describe_stream(StreamName=stream_name)
            self.shards = response['StreamDescription']['Shards']
            
            self.analytics_store = []
            
        def process_shard(self, shard_id):
            # Get a shard iterator
            iterator_response = self.client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=shard_id,
                ShardIteratorType='LATEST'
            )
            
            shard_iterator = iterator_response['ShardIterator']
            
            while self.running and shard_iterator:
                try:
                    response = self.client.get_records(
                        ShardIterator=shard_iterator,
                        Limit=100
                    )
                    
                    records = response['Records']
                    
                    for record in records:
                        analytics = json.loads(record['Data'].decode('utf-8'))
                        self.analytics_store.append(analytics)
                        print(f"\nAnalytics snapshot at: {analytics['timestamp']}")
                        print(f"Active Users: {analytics['active_users']}")
                        if analytics['top_categories']:
                            print(f"Top Category: {analytics['top_categories'][0]['category']} " 
                                  f"({analytics['top_categories'][0]['count']} events)")
                        else:
                            print("No categories yet")
                        
                        if analytics['top_converting_products']:
                            top_product = analytics['top_converting_products'][0]
                            print(f"Top Converting Product: {top_product['product_id']} " 
                                  f"({top_product['conversion_rate']}% conversion)")
                        else:
                            print("No conversion data yet")
                        print("---")
                    
                    shard_iterator = response.get('NextShardIterator')
                    
                    # Avoid throttling
                    if not records:
                        time.sleep(1)
                    else:
                        time.sleep(0.1)
                except Exception as e:
                    print(f"Analytics consumer error in shard {shard_id}: {e}")
                    time.sleep(1)  # Brief pause before retrying
                    
                    # Try to get a new iterator
                    try:
                        iterator_response = self.client.get_shard_iterator(
                            StreamName=self.stream_name,
                            ShardId=shard_id,
                            ShardIteratorType='LATEST'
                        )
                        shard_iterator = iterator_response['ShardIterator']
                    except Exception:
                        # If we can't get a new iterator, break the loop
                        break
                    
        def start(self):
            self.running = True
            self.threads = []
            
            # Start a thread for each shard
            for shard in self.shards:
                thread = threading.Thread(
                    target=self.process_shard,
                    args=(shard['ShardId'],)
                )
                thread.start()
                self.threads.append(thread)
            
            print(f"Analytics consumer started with {len(self.threads)} shard processors")
                
        def stop(self):
            self.running = False
            for thread in self.threads:
                thread.join()
            print("Analytics consumer stopped")
    
    try:
        # Set up the pipeline components
        data_source = KinesisDataSource(client, config['stream_name'])
        processor = KinesisProcessor(client, config['stream_name'], config['analytics_stream'])
        analytics_consumer = KinesisAnalyticsConsumer(client, config['analytics_stream'])
        
        # Start the pipeline
        analytics_consumer.start()
        processor.start()
        data_source.start(events_per_second=10)
        
        # Let it run for a while
        print("\nKinesis analytics pipeline is running...")
        print("Press Ctrl+C to stop after you've observed enough data")
        
        # Keep the main thread running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nShutting down the analytics pipeline...")
    
    except Exception as e:
        print(f"Error in analytics pipeline: {e}")
        if config['use_localstack']:
            print("If using LocalStack, check if it's running correctly.")
        else:
            print("If using AWS, check your AWS credentials and configuration.")
    
    finally:
        # Clean up
        if 'data_source' in locals():
            data_source.stop()
        if 'processor' in locals():
            processor.stop()
        if 'analytics_consumer' in locals():
            analytics_consumer.stop()
        
        print("\nAnalytics pipeline shut down successfully.")
        
        # Return a summary of the analytics
        print("\nAnalytics Summary:")
        if 'analytics_consumer' in locals() and analytics_consumer.analytics_store:
            last_snapshot = analytics_consumer.analytics_store[-1]
            print(f"Final active users count: {last_snapshot['active_users']}")
            
            if last_snapshot['top_categories']:
                top_cats = ", ".join([f"{c['category']} ({c['count']})" 
                                    for c in last_snapshot['top_categories'][:3]])
                print(f"Top 3 categories: {top_cats}")
            
            if last_snapshot['top_converting_products']:
                top_prods = ", ".join([f"{p['product_id']} ({p['conversion_rate']}%)" 
                                    for p in last_snapshot['top_converting_products'][:3]])
                print(f"Top 3 converting products: {top_prods}")
        else:
            print("No analytics data collected.")

def main():
    """Parse command line arguments and run the appropriate example."""
    parser = argparse.ArgumentParser(description='Kinesis examples')
    parser.add_argument('example', choices=['producer', 'consumer', 'analytics'],
                        help='Which example to run')
    parser.add_argument('--aws', action='store_true', 
                       help='Use real AWS (may incur charges) instead of LocalStack')
    
    args = parser.parse_args()
    
    # Set environment variable based on command line argument
    if args.aws:
        os.environ['USE_LOCALSTACK'] = 'False'
        print("WARNING: Using real AWS services - charges may apply")
    else:
        os.environ['USE_LOCALSTACK'] = 'True'
    
    if args.example == 'producer':
        kinesis_producer_example()
    elif args.example == 'consumer':
        kinesis_consumer_example()
    elif args.example == 'analytics':
        kinesis_analytics_pipeline()

if __name__ == '__main__':
    main()