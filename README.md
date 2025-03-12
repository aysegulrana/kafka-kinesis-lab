# Kafka and Kinesis Lab

This repository contains practical examples and code for learning about Apache Kafka and Amazon Kinesis streaming platforms. The lab is designed to be completed in approximately one hour and covers the basics of both platforms with hands-on examples.

## Important: Cost-Free Implementation

This lab has been designed to run **completely free of charge** by using:
1. Docker for Kafka (or local Kafka on macOS)
2. LocalStack for AWS Kinesis simulation

You will **not** need to use real AWS services or incur any charges. Everything runs locally on your machine.

## Prerequisites

- Python 3.6+
- Docker and Docker Compose
- Basic understanding of messaging systems

## Complete Setup Instructions (From Scratch)

### 1. Install Python (if not already installed)

#### Windows:
1. Download the installer from [python.org](https://www.python.org/downloads/)
2. Run the installer and check "Add Python to PATH"
3. Verify installation by opening Command Prompt and typing: `python --version`

#### macOS:
```bash
brew install python
```
Or download from [python.org](https://www.python.org/downloads/)

#### Linux (Ubuntu/Debian):
```bash
sudo apt update
sudo apt install python3 python3-pip
```

### 2. Install Docker and Docker Compose

#### Windows:
1. Download [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop)
2. Run the installer and follow the prompts
3. After installation, Docker Compose is included
4. Verify installation: `docker --version` and `docker compose --version`

#### macOS:
1. Download [Docker Desktop for Mac](https://www.docker.com/products/docker-desktop)
2. Install the application and launch it
3. After installation, Docker Compose is included
4. Verify installation: `docker --version` and `docker compose --version`

#### Linux (Ubuntu/Debian):
```bash
# Install Docker
sudo apt update
sudo apt install docker.io

# Start and enable Docker
sudo systemctl start docker
sudo systemctl enable docker

# Add your user to the docker group (to run Docker without sudo)
sudo usermod -aG docker $USER
# Log out and log back in for this to take effect

# Install Docker Compose
sudo apt install docker-compose

# Verify installation
docker --version
docker-compose --version
```

### 3. Set up LocalStack (Instead of Real AWS)

Instead of requiring a real AWS account, this lab uses LocalStack, which is an open-source tool that provides a local, fully functional AWS cloud stack for testing and development.

No AWS account is needed, and no charges will be incurred. All AWS services are simulated locally using Docker.

When you start the lab with Docker Compose, LocalStack will automatically be set up with:
- Kinesis service emulation
- Pre-configured streams
- Default access credentials

**Note:** The default configuration in this lab uses LocalStack by default. You do not need to set up AWS credentials unless you explicitly want to use real AWS services (which is not recommended for this lab).

### 4. Clone the Repository (or download files)

```bash
# If you have git installed:
git clone https://github.com/yourusername/kafka-kinesis-lab.git
cd kafka-kinesis-lab

# Alternatively, download and extract the zip file from the repository
```

### 5. Install Python Dependencies

```bash
# Option 1: Install globally
pip install -r requirements.txt

# Option 2: Set up a virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Setup Based on Operating System

### Standard Setup (Windows/Linux)

```bash
# Start Kafka, Zookeeper, and LocalStack using Docker Compose
docker compose up -d

# Wait about 30 seconds for services to initialize
```

### macOS Setup (Alternative)

macOS users may experience issues with Docker networking and Kafka. If you encounter problems, follow these alternative steps:

#### Automated Setup for macOS (Recommended)

We've created an automated setup script for macOS that will:
1. Set up a Python virtual environment
2. Install all dependencies
3. Install Kafka and ZooKeeper using Homebrew
4. Configure LocalStack for Kinesis
5. Create helper scripts to start and stop the lab environment

To use the automated setup:

```bash
# Make the script executable
chmod +x setup_mac.sh

# Run the setup script
./setup_mac.sh
```

After running the setup script, you can:
- Start the lab: `source ./activate_lab.sh`
- Stop the lab: `./stop_lab.sh`

#### Manual Setup for macOS

If you prefer to set up manually on macOS:

Start the services:
   ```bash
   docker compose up -d 

   #check if all three services (kafka, zookeeper, localstack) are running
   docker ps
   ```

## Running the Examples

### Kafka Examples

Before running the examples, make sure you're in the same environment where you did the requirements installation. If you used a virtual environment, activate it first.
```bash
source venv/bin/activate
```

1. Run the Kafka producer:

```bash
python kafka_examples.py producer
```

2. In a separate terminal, run the Kafka consumer:

```bash
python kafka_examples.py consumer
```

3. To run the Kafka analytics pipeline example:

```bash
python kafka_examples.py analytics
```

### Kinesis Examples (using LocalStack - no AWS charges)

1. Run the Kinesis producer:

```bash
python kinesis_examples.py producer
```

2. In a separate terminal, run the Kinesis consumer:

```bash
python kinesis_examples.py consumer
```

3. To run the Kinesis analytics pipeline example:

```bash
python kinesis_examples.py analytics
```

### Kafka to Kinesis Bridge

To demonstrate streaming data from Kafka to Kinesis:

```bash
python kafka_to_kinesis.py
```

### Performance Testing

To run performance tests comparing Kafka and Kinesis:

```bash
python performance_tests.py
```

This will conduct throughput and latency tests for both producers and consumers, using LocalStack for Kinesis (no AWS charges).

## Lab Architecture

This lab uses the following architecture:

```
┌────────────────┐     ┌────────────────┐     ┌────────────────┐
│                │     │                │     │                │
│  Kafka         │     │  LocalStack    │     │  Python        │
│  (Docker/Local)│     │  (Docker)      │     │  Examples      │
│                │     │                │     │                │
│  - Zookeeper   │     │  - Kinesis     │     │  - Producer    │
│  - Broker      │     │    simulation  │     │  - Consumer    │
│                │     │                │     │  - Analytics   │
└───────┬────────┘     └───────┬────────┘     └───────┬────────┘
        │                      │                      │
        └──────────────────────┼──────────────────────┘
                               │
                      ┌────────┴────────┐
                      │                 │
                      │  Kafka-to-      │
                      │  Kinesis Bridge │
                      │                 │
                      └─────────────────┘
```

## Lab Structure

- `docker-compose.yml` - Docker configuration for Kafka and LocalStack
- `docker-compose-simple.yml` - Simplified Docker configuration for LocalStack only (macOS)
- `setup_mac.sh` - Automated setup script for macOS
- `requirements.txt` - Python dependencies
- `config.py` - Configuration for Kafka and Kinesis/LocalStack
- `kafka_examples.py` - Examples for Kafka producer, consumer, and analytics
- `kinesis_examples.py` - Examples for Kinesis producer, consumer, and analytics
- `kafka_to_kinesis.py` - Example showing how to bridge Kafka to Kinesis
- `performance_tests.py` - Script to test and compare Kafka and Kinesis performance

## Key Concepts Covered

- Setting up Kafka and Kinesis environments
- Message production and consumption in both platforms
- Partitioning (Kafka) and sharding (Kinesis)
- Real-time analytics pipeline implementation
- Fault tolerance and scalability
- AWS service emulation using LocalStack

## Troubleshooting

### Docker Issues

1. **Docker service not starting:**
   ```bash
   # Check Docker service status
   systemctl status docker  # Linux
   
   # Try restarting Docker
   sudo systemctl restart docker  # Linux
   # Or restart Docker Desktop on Windows/Mac
   ```

2. **Permission denied errors:**
   ```bash
   # Add your user to the docker group
   sudo usermod -aG docker $USER
   # Log out and log back in for this to take effect
   ```

3. **Port conflicts:**
   ```bash
   # Check if something is using the required ports
   # Linux/macOS:
   lsof -i:9092
   lsof -i:2181
   
   # Windows:
   netstat -ano | findstr 9092
   netstat -ano | findstr 2181
   ```

### macOS-specific Kafka Issues

1. **Ensure Kafka and ZooKeeper are running:**
   ```bash
   # Check if ZooKeeper is running
   lsof -i:2181
   
   # Check if Kafka is running
   lsof -i:9092
   
   # If not running, start them again
   zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties &
   kafka-server-start /usr/local/etc/kafka/server.properties &
   ```

2. **"NoBrokersAvailable" error on macOS:**
   Edit your `/usr/local/etc/kafka/server.properties` file to ensure:
   ```
   advertised.listeners=PLAINTEXT://localhost:9092
   listeners=PLAINTEXT://0.0.0.0:9092
   ```

### LocalStack Issues

1. **Ensure LocalStack is running:**
   ```bash
   docker ps | grep localstack
   
   # If not running, start it
   docker compose up -d localstack  # Standard setup
   # or
   docker compose -f docker-compose-simple.yml up -d  # macOS setup
   ```

2. **Testing LocalStack connectivity:**
   ```bash
   curl http://localhost:4566/health
   
   # Create a Kinesis stream manually
   aws --endpoint-url=http://localhost:4566 kinesis create-stream --stream-name test-stream --shard-count 1
   
   # List streams
   aws --endpoint-url=http://localhost:4566 kinesis list-streams
   ```

### Python Issues

1. **Module not found errors:**
   ```bash
   # Ensure you've installed all dependencies
   pip install -r requirements.txt
   
   # If using a virtual environment, make sure it's activated
   source venv/bin/activate  # Linux/macOS
   venv\Scripts\activate     # Windows
   ```

2. **Permission errors on Linux/macOS:**
   ```bash
   # Make scripts executable
   chmod +x *.py
   ```

## Cleaning Up

### Standard Cleanup (Windows/Linux)

```bash
# Stop all Docker containers
docker compose down
```

### macOS Cleanup

If you used the macOS-specific setup:

```bash
# Option 1: Use the provided cleanup script
./stop_lab.sh

# Option 2: Manual cleanup
# Stop Kafka and ZooKeeper
pkill -f kafka
pkill -f zookeeper

# Stop LocalStack
docker compose -f docker-compose-simple.yml down
```

## Additional Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Amazon Kinesis Documentation](https://docs.aws.amazon.com/kinesis/index.html)
- [kafka-python Documentation](https://kafka-python.readthedocs.io/)
- [boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [Docker Documentation](https://docs.docker.com/)
- [LocalStack Documentation](https://docs.localstack.cloud/)