#!/bin/bash
# Initialize Kinesis streams in LocalStack
awslocal kinesis create-stream --stream-name data-stream --shard-count 1
awslocal kinesis create-stream --stream-name analytics --shard-count 1