#!/bin/bash
# Script to activate the virtual environment and start services

# Activate virtual environment
source venv/bin/activate

# Check if services are already running
if lsof -i:9092 > /dev/null || lsof -i:2181 > /dev/null; then
    echo "Kafka and/or ZooKeeper services are already running."
    echo "Do you want to stop them and restart? (y/n)"
    read RESTART_SERVICES
    if [[ $RESTART_SERVICES == "y" || $RESTART_SERVICES == "Y" ]]; then
        echo "Stopping Kafka and ZooKeeper..."
        pkill -f kafka
        pkill -f zookeeper
        sleep 5
    else
        echo "Using existing Kafka and ZooKeeper services."
    fi
fi

# Check if LocalStack is running
if ! docker ps | grep localstack > /dev/null; then
    echo "LocalStack is not running. Starting LocalStack..."
    docker compose -f docker-compose-simple.yml up -d
    sleep 5
else
    echo "LocalStack is already running."
fi

# Start ZooKeeper and Kafka if needed
if ! lsof -i:2181 > /dev/null; then
    echo "Starting ZooKeeper..."
    zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties > /dev/null 2>&1 &
    echo "Waiting for ZooKeeper to start..."
    sleep 10
else
    echo "ZooKeeper is already running."
fi

if ! lsof -i:9092 > /dev/null; then
    echo "Starting Kafka..."
    kafka-server-start /usr/local/etc/kafka/server.properties > /dev/null 2>&1 &
    echo "Waiting for Kafka to start..."
    sleep 10
else
    echo "Kafka is already running."
fi

echo ""
echo "=== Lab Environment is Ready ==="
echo ""
echo "The virtual environment is activated and all services are running."
echo "You can now run the Python examples."
echo ""
echo "To deactivate the virtual environment when done, type: deactivate"
echo "To stop the services when done, run: ./stop_lab.sh"
echo ""
