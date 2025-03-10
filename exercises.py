# ===== EXERCISE SECTION =====

def lab_exercises():
    """
    Provide hands-on exercises for the lab.
    """
    print("\n====== LAB EXERCISES ======")
    
    """
    # Exercise 1: Basic Producer/Consumer
    
    Implement a basic producer and consumer for both Kafka and Kinesis. Produce 100 messages and consume them, 
    printing out the message contents.
    
    *Kafka Implementation:*
    """
    # TODO: Implement a Kafka producer that sends 100 messages
    # TODO: Implement a Kafka consumer that reads those messages

    """
    *Kinesis Implementation:*
    """
    # TODO: Implement a Kinesis producer that sends 100 messages
    # TODO: Implement a Kinesis consumer that reads those messages

    
    # Exercise 2: Dead Letter Queue
    """
    Implement a dead letter queue pattern for messages that fail processing.
    
    *Kafka Implementation:*
    """
    # TODO: Implement a Kafka consumer that processes messages and sends failed ones to a dead-letter topic
    # TODO: Implement a consumer for the dead-letter topic

    """
    *Kinesis Implementation:*
    """
    # TODO: Implement a Kinesis consumer that processes messages and sends failed ones to a dead-letter stream
    # TODO: Implement a consumer for the dead-letter stream

    
    # Exercise 3: Partitioning/Sharding Strategy
    """
    Implement a custom partitioning/sharding strategy that routes messages based on a specific field in the data.
    
    *Kafka Implementation:*
    """
    # TODO: Implement a custom partitioner for Kafka
    # TODO: Use the custom partitioner in a producer

    """
    *Kinesis Implementation:*
    """
    # TODO: Implement a custom partition key selection strategy for Kinesis
    # TODO: Use the strategy in a producer

    
    # Exercise 4: Error Handling and Retry Logic
    """
    Implement robust error handling and retry logic for both systems.
    
    *Kafka Implementation:*
    """
    # TODO: Implement a Kafka producer with retry logic
    # TODO: Implement a Kafka consumer with error handling

    """
    *Kinesis Implementation:*
    """
    # TODO: Implement a Kinesis producer with retry logic
    # TODO: Implement a Kinesis consumer with error handling

    
    # Challenge Exercise: Exactly-Once Processing
    """
    Implement an exactly-once processing pattern for both systems.
    
    *Kafka Implementation:*
    """
    # TODO: Implement exactly-once processing using Kafka transactions

    """
    *Kinesis Implementation:*
    """
    # TODO: Implement exactly-once processing using checkpointing in DynamoDB