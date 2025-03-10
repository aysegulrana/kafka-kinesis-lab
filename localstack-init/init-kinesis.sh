#!/bin/bash
# This script initializes Kinesis streams in LocalStack

# Create Kinesis streams
echo "Creating Kinesis streams in LocalStack..."
awslocal kinesis create-stream --stream-name data-stream --shard-count 1
awslocal kinesis create-stream --stream-name analytics --shard-count 1

# Wait for streams to be active
sleep 2

# List created streams
echo "Listing created Kinesis streams:"
awslocal kinesis list-streams

echo "LocalStack Kinesis initialization complete!"