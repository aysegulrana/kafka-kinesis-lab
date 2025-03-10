from confluent_kafka import Producer
import sys
import json

def delivery_report(err, msg):
    """
    Callback function to check the delivery of messages
    """
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def kafka_producer_example(topic_name='example_topic'):
    """
    Create a Kafka producer and send a message
    """
    print("====== KAFKA PRODUCER EXAMPLE ======")
    
    # Correct bootstrap servers configuration for local development
    conf = {
        'bootstrap.servers': 'localhost:29092',  # Use the host-mapped port
        'client.id': 'python-producer'
    }

    try:
        # Create Producer instance
        producer = Producer(conf)

        # Produce a sample message
        topic = topic_name
        message = {
            'message': 'Hello, Kafka!',
            'timestamp': '2024-03-10'
        }

        # Serialize the message to JSON
        serialized_message = json.dumps(message).encode('utf-8')

        # Produce the message
        producer.produce(topic, value=serialized_message, callback=delivery_report)

        # Wait for any outstanding messages to be delivered
        producer.flush()

    except Exception as e:
        print(f"Unexpected error in producer: {type(e).__name__} - {str(e)}")
        raise

def main():
    if len(sys.argv) > 1 and sys.argv[1] == 'producer':
        # Create a topic before producing
        kafka_producer_example()

if __name__ == '__main__':
    main()