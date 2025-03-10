#!/usr/bin/env python
"""
Examples of using Apache Kafka for data streaming.
This file contains three main examples:
1. Kafka Producer
2. Kafka Consumer
3. Kafka Real-time Analytics Pipeline

Run with:
- python kafka_examples.py producer
- python kafka_examples.py consumer
- python kafka_examples.py analytics
"""

import argparse
import json
import time
import random
import uuid
import threading
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
import statistics

from config import get_kafka_config

# --- KAFKA PRODUCER ---
def kafka_producer_example():
    """Example implementation of a Kafka producer with network diagnostics."""
    print("\n====== KAFKA PRODUCER EXAMPLE ======")
    
    # Get configuration
    config = get_kafka_config()
    
    try:
        from kafka.errors import KafkaError
        import socket
        
        print(f"Attempting to connect to bootstrap servers: {config['bootstrap_servers']}")
        
        # Enhanced connection check with more detailed error reporting
        def check_connection(host, port):
            try:
                print(f"Attempting socket connection to {host}:{port}")
                sock = socket.create_connection((host, port), timeout=10)
                sock.close()
                return True
            except Exception as e:
                print(f"Connection error to {host}:{port}: {type(e).__name__} - {e}")
                return False
        
        # Verify network connectivity
        connection_successful = False
        for server in config['bootstrap_servers']:
            host, port = server.split(':')
            is_connected = check_connection(host, int(port))
            print(f"Server {server}: {'Connected' if is_connected else 'Not Connected'}")
            if is_connected:
                connection_successful = True
        
        if not connection_successful:
            raise ConnectionError("Unable to connect to any bootstrap servers")
        
        # Create producer with extensive configuration
        from kafka import KafkaProducer
        
        producer = KafkaProducer(
            bootstrap_servers=config['bootstrap_servers'],
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
        
        def generate_data():
            """Generate a sample data record."""
            return {
                'id': str(uuid.uuid4()),
                'timestamp': datetime.now().isoformat(),
                'value': random.randint(1, 100),
                'category': random.choice(['A', 'B', 'C']),
                'metadata': {
                    'source': 'kafka-producer',
                    'version': '1.0'
                }
            }
        
        print("Sending messages to Kafka topic:", config['topic'])
        for i in range(10):  # Send 10 messages
            data = generate_data()
            
            # Kafka allows specifying a partition key for custom routing
            partition_key = data['category'].encode('utf-8')
            
            try:
                # Send the message to the topic with the specified key
                print(f"Attempting to send message {i+1}")
                future = producer.send(config['topic'], key=partition_key, value=data)
                
                # The result() method blocks until the message is sent (or fails)
                record_metadata = future.get(timeout=30)
                
                print(f"Message {i+1} sent successfully to {record_metadata.topic} "
                      f"partition {record_metadata.partition} "
                      f"offset {record_metadata.offset}")
            
            except Exception as send_err:
                print(f"Error sending message {i+1}: {type(send_err).__name__} - {send_err}")
                import traceback
                traceback.print_exc()
            
            time.sleep(0.5)
    
    except Exception as e:
        print(f"Unexpected error in producer: {type(e).__name__} - {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Always remember to flush and close
        if 'producer' in locals():
            producer.flush()
            producer.close()
        print("Producer closed.")

# --- KAFKA CONSUMER ---
def kafka_consumer_example():
    """Example implementation of a Kafka consumer."""
    print("\n====== KAFKA CONSUMER EXAMPLE ======")
    
    # Get configuration
    config = get_kafka_config()
    
    # Create a consumer that deserializes data from JSON
    consumer = KafkaConsumer(
        config['topic'],
        bootstrap_servers=config['bootstrap_servers'],
        group_id=config['group_id'],
        auto_offset_reset='earliest',  # Start from the beginning if no committed offset
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        # Kafka-specific configurations
        enable_auto_commit=True,    # Automatically commit offsets
        auto_commit_interval_ms=1000,  # Commit interval
        session_timeout_ms=30000,   # Consumer group session timeout
        max_poll_records=100        # Max records per poll
    )
    
    try:
        print(f"Consuming messages from Kafka topic: {config['topic']}")
        print("Press Ctrl+C to stop...")
        
        # Kafka consumers can be used as iterators
        for message in consumer:
            print(f"\nReceived message from {message.topic}, partition {message.partition}, "
                  f"offset {message.offset}")
            print(f"Key: {message.key.decode('utf-8') if message.key else 'None'}")
            print(f"Value: {message.value}")
            print(f"Timestamp: {message.timestamp}")
            print("---")
    
    except KeyboardInterrupt:
        print("\nConsumer stopped by user")
    
    finally:
        consumer.close()
        print("Consumer closed.")

# --- KAFKA REAL-TIME ANALYTICS PIPELINE ---
def kafka_analytics_pipeline():
    """
    Example of a real-time analytics pipeline using Kafka.
    
    This example demonstrates:
    1. A data source generating events
    2. A processor analyzing events in real-time
    3. A consumer storing and displaying analytics results
    """
    print("\n====== KAFKA REAL-TIME ANALYTICS PIPELINE ======")
    
    # Get configuration
    config = get_kafka_config()
    
    # Simulated data source - e.g., web clickstream, IoT sensors, etc.
    class DataSource:
        def __init__(self, producer, topic):
            self.producer = producer
            self.topic = topic
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
                    event = self.generate_event()
                    self.producer.send(
                        self.topic, 
                        key=event['user_id'].encode('utf-8'),
                        value=event
                    )
                    time.sleep(1 / events_per_second)
            
            self.thread = threading.Thread(target=_run)
            self.thread.start()
            print(f"Data source started - generating {events_per_second} events/second")
            
        def stop(self):
            self.running = False
            if hasattr(self, 'thread'):
                self.thread.join()
            print("Data source stopped")
    
    # Real-time processor - analyzes events as they arrive
    class RealTimeProcessor:
        def __init__(self, bootstrap_servers, input_topic, output_topic, group_id):
            self.consumer = KafkaConsumer(
                input_topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest'
            )
            
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            self.output_topic = output_topic
            self.running = False
            
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
            
        def start(self):
            self.running = True
            
            def _run():
                while self.running:
                    try:
                        for message in self.consumer:
                            if not self.running:
                                break
                                
                            event = message.value
                            analytics = self.process_event(event)
                            
                            # Send analytics to output topic
                            self.producer.send(self.output_topic, value=analytics)
                    except Exception as e:
                        print(f"Processor error: {e}")
                        time.sleep(1)  # Brief pause before retrying
                        
            self.thread = threading.Thread(target=_run)
            self.thread.start()
            print("Real-time processor started")
            
        def stop(self):
            self.running = False
            self.consumer.close()
            self.producer.close()
            if hasattr(self, 'thread'):
                self.thread.join()
            print("Processor stopped")
    
    # Analytics Consumer - stores results
    class AnalyticsConsumer:
        def __init__(self, bootstrap_servers, topic, group_id):
            self.consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest'
            )
            
            self.running = False
            self.analytics_store = []
            
        def start(self):
            self.running = True
            
            def _run():
                while self.running:
                    try:
                        for message in self.consumer:
                            if not self.running:
                                break
                                
                            analytics = message.value
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
                    except Exception as e:
                        print(f"Analytics consumer error: {e}")
                        time.sleep(1)  # Brief pause before retrying
            
            self.thread = threading.Thread(target=_run)
            self.thread.start()
            print("Analytics consumer started")
            
        def stop(self):
            self.running = False
            self.consumer.close()
            if hasattr(self, 'thread'):
                self.thread.join()
            print("Analytics consumer stopped")
    
    # Initialize Kafka components
    bootstrap_servers = config['bootstrap_servers']
    
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Set up the pipeline components
    data_source = DataSource(producer, config['topic'])
    processor = RealTimeProcessor(bootstrap_servers, config['topic'], 
                                  config['analytics_topic'], 'processor-group')
    analytics_consumer = AnalyticsConsumer(bootstrap_servers, config['analytics_topic'], 
                                          'analytics-group')
    
    try:
        # Start the pipeline
        analytics_consumer.start()
        processor.start()
        data_source.start(events_per_second=10)
        
        # Let it run for a while
        print("\nKafka analytics pipeline is running...")
        print("Press Ctrl+C to stop after you've observed enough data")
        
        # Keep the main thread running
        while True:
            time.sleep(1)
        
    except KeyboardInterrupt:
        print("\nShutting down the analytics pipeline...")
    
    finally:
        # Clean up
        data_source.stop()
        processor.stop()
        analytics_consumer.stop()
        producer.close()
        
        print("\nAnalytics pipeline shut down successfully.")
        
        # Return a summary of the analytics
        print("\nAnalytics Summary:")
        if analytics_consumer.analytics_store:
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
    parser = argparse.ArgumentParser(description='Kafka examples')
    parser.add_argument('example', choices=['producer', 'consumer', 'analytics'],
                        help='Which example to run')
    
    args = parser.parse_args()
    
    if args.example == 'producer':
        kafka_producer_example()
    elif args.example == 'consumer':
        kafka_consumer_example()
    elif args.example == 'analytics':
        kafka_analytics_pipeline()

if __name__ == '__main__':
    main()