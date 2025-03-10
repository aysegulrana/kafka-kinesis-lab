#!/bin/bash
# Script to stop all lab services

echo "Stopping Kafka and ZooKeeper..."
pkill -f kafka
pkill -f zookeeper

echo "Stopping LocalStack..."
docker compose -f docker-compose-simple.yml down

echo "All services have been stopped."
echo "To restart the lab environment, run: source ./activate_lab.sh"
