#!/bin/bash
# Setup script for Kafka and Kinesis Lab on macOS with virtual environment

echo "=== Setting up Kafka and Kinesis Lab for macOS ==="

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install Docker Desktop for Mac first."
    echo "Download from: https://www.docker.com/products/docker-desktop"
    exit 1
fi

# Check if Homebrew is installed
if ! command -v brew &> /dev/null; then
    echo "Homebrew is not installed. Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
else
    echo "Homebrew is already installed."
fi

# Install Python if not already installed
if ! command -v python3 &> /dev/null; then
    echo "Python 3 is not installed. Installing Python 3..."
    brew install python
else
    echo "Python 3 is already installed."
fi

# Set up virtual environment
echo "Setting up Python virtual environment..."
# Check if venv exists
if [ -d "venv" ]; then
    echo "Virtual environment already exists. Recreate it? (y/n)"
    read RECREATE_VENV
    if [[ $RECREATE_VENV == "y" || $RECREATE_VENV == "Y" ]]; then
        echo "Removing existing virtual environment..."
        rm -rf venv
        python3 -m venv venv
        echo "Virtual environment recreated."
    else
        echo "Using existing virtual environment."
    fi
else
    python3 -m venv venv
    echo "Virtual environment created."
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install Python dependencies in the virtual environment
echo "Installing Python dependencies in the virtual environment..."
pip install --upgrade pip
pip install -r requirements.txt

# Install Kafka
echo "Installing Kafka with Homebrew..."
brew install kafka

# Make sure LocalStack initialization script is executable
chmod +x localstack-init/init-kinesis.sh

# Update config.py with macOS-specific settings
echo "Updating config.py with macOS-specific settings..."
cp config.py config.py.bak
cat > config.py << EOF
"""
Configuration settings for Kafka and Kinesis.
"""

import os

# Kafka Configuration - Set to use local Kafka installation from Homebrew
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'topic': 'data-stream',
    'group_id': 'data-processing-group',
    'client_id': 'kafka-python-client',
    'analytics_topic': 'analytics'
}

# Kinesis Configuration
# Set to use LocalStack by default to avoid AWS charges
KINESIS_CONFIG = {
    'stream_name': 'data-stream',
    'region_name': os.environ.get('AWS_DEFAULT_REGION', 'us-east-1'),
    'aws_access_key_id': os.environ.get('AWS_ACCESS_KEY_ID', 'test'),
    'aws_secret_access_key': os.environ.get('AWS_SECRET_ACCESS_KEY', 'test'),
    'shard_count': 1,
    'analytics_stream': 'analytics',
    'use_localstack': True,
    'endpoint_url': 'http://localhost:4566'  # LocalStack endpoint
}

# Kafka to Kinesis Bridge Configuration
BRIDGE_CONFIG = {
    'kafka_topic': 'data-stream',
    'kinesis_stream': 'data-stream'
}

def get_kafka_config():
    """Returns the Kafka configuration settings."""
    return KAFKA_CONFIG

def get_kinesis_config():
    """Returns the Kinesis configuration settings."""
    return KINESIS_CONFIG

def get_bridge_config():
    """Returns the Kafka to Kinesis bridge configuration settings."""
    return BRIDGE_CONFIG
EOF

# Create activation script for the lab
echo "Creating activation script for the lab..."
cat > activate_lab.sh << EOF
#!/bin/bash
# Script to activate the virtual environment and start services

# Activate virtual environment
source venv/bin/activate

# Check if services are already running
if lsof -i:9092 > /dev/null || lsof -i:2181 > /dev/null; then
    echo "Kafka and/or ZooKeeper services are already running."
    echo "Do you want to stop them and restart? (y/n)"
    read RESTART_SERVICES
    if [[ \$RESTART_SERVICES == "y" || \$RESTART_SERVICES == "Y" ]]; then
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
EOF

# Create deactivation script for the lab
echo "Creating script to stop services..."
cat > stop_lab.sh << EOF
#!/bin/bash
# Script to stop all lab services

echo "Stopping Kafka and ZooKeeper..."
pkill -f kafka
pkill -f zookeeper

echo "Stopping LocalStack..."
docker compose -f docker-compose-simple.yml down

echo "All services have been stopped."
echo "To restart the lab environment, run: source ./activate_lab.sh"
EOF

# Make the scripts executable
chmod +x activate_lab.sh
chmod +x stop_lab.sh

echo "=== Setup completed ==="
echo ""
echo "To start the lab environment:"
echo "  source ./activate_lab.sh"
echo ""
echo "To stop the lab environment when done:"
echo "  ./stop_lab.sh"
echo ""
echo "Would you like to start the lab environment now? (y/n)"
read START_LAB

if [[ $START_LAB == "y" || $START_LAB == "Y" ]]; then
    source ./activate_lab.sh
else
    echo "Lab environment not started. You can start it later using the command above."
    echo "Don't forget to activate the virtual environment with: source venv/bin/activate"
fi