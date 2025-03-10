# Kafka and Kinesis Lab

This repository contains practical examples and code for learning about Apache Kafka and Amazon Kinesis streaming platforms. The lab is designed to be completed in approximately one hour and covers the basics of both platforms with hands-on examples.

## Important: Cost-Free Implementation

This lab has been designed to run **completely free of charge** by using:
1. Docker for Kafka
2. LocalStack for AWS Kinesis simulation

You will **not** need to use real AWS services or incur any charges. Everything runs locally on your machine.

## Prerequisites

- Python 3.6+
- Docker and Docker Compose
- Basic understanding of messaging systems

## Complete Setup Instructions (From Scratch)# Kafka and Kinesis Lab

This repository contains practical examples and code for learning about Apache Kafka and Amazon Kinesis streaming platforms. The lab is designed to be completed in approximately one hour and covers the basics of both platforms with hands-on examples.

## Prerequisites

- Python 3.6+
- AWS account (for Kinesis examples)
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
4. Verify installation: `docker --version` and `docker-compose --version`

#### macOS:
1. Download [Docker Desktop for Mac](https://www.docker.com/products/docker-desktop)
2. Install the application and launch it
3. After installation, Docker Compose is included
4. Verify installation: `docker --version` and `docker-compose --version`

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
git clone https://github.com/aysegulrana/kafka-kinesis-lab.git
cd kafka-kinesis-lab

# Alternatively, download and extract the zip file from the repository
```

### 5. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 6. Start Docker Containers (Kafka & LocalStack)

Start Kafka, Zookeeper, and LocalStack using Docker Compose:

```bash
docker-compose up -d
```

Verify all services are running:
```bash
docker ps
```
You should see containers for Kafka, Zookeeper, and LocalStack running.

Wait about 30 seconds for the services to fully initialize. LocalStack will automatically create the necessary Kinesis streams during startup.

## Running the Examples

### Kafka Examples

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

### Performance Testing

To run performance tests comparing Kafka and Kinesis:

```bash
python performance_tests.py
```

This will conduct throughput and latency tests for both producers and consumers, using LocalStack for Kinesis (no AWS charges).

## Lab Architecture

- `docker-compose.yml` - Docker configuration for Kafka
- `requirements.txt` - Python dependencies
- `config.py` - Configuration for Kafka and Kinesis
- `kafka_examples.py` - Examples for Kafka producer, consumer, and analytics
- `kinesis_examples.py` - Examples for Kinesis producer, consumer, and analytics
- `kafka_to_kinesis.py` - Example showing how to bridge Kafka to Kinesis

## Key Concepts Covered

- Setting up Kafka and Kinesis
- Message production and consumption in both platforms
- Partitioning and sharding
- Real-time analytics pipeline implementation
- Fault tolerance and scalability

## Troubleshooting

### Docker Issues

1. **Docker service not starting:**
   ```bash
   # Check Docker service status
   systemctl status docker
   
   # Try restarting Docker
   sudo systemctl restart docker
   ```

2. **Permission denied errors:**
   ```bash
   # Add your user to the docker group
   sudo usermod -aG docker $USER
   # Log out and log back in
   ```

3. **Port conflicts:**
   ```bash
   # Check if something is using the required ports
   sudo netstat -tulpn | grep 9092
   sudo netstat -tulpn | grep 2181
   
   # Stop the conflicting service or change the port in docker-compose.yml
   ```

### AWS/Kinesis Issues

1. **AWS credentials not found:**
   ```bash
   # Check if credentials file exists
   ls -la ~/.aws/
   
   # Manually create/edit the credentials file
   mkdir -p ~/.aws
   nano ~/.aws/credentials
   
   # Add your credentials:
   [default]
   aws_access_key_id=YOUR_ACCESS_KEY
   aws_secret_access_key=YOUR_SECRET_KEY
   ```

2. **Kinesis "resource not found" errors:**
   - Ensure your IAM user has the correct permissions
   - Check if you're in the correct AWS region
   - Verify the stream name is correct

3. **Throttling errors:**
   - Reduce the rate of requests in the examples
   - Request a service limit increase for Kinesis

### Python Issues

1. **Module not found errors:**
   ```bash
   # Ensure you've installed all dependencies
   pip install -r requirements.txt
   
   # If using a virtual environment, make sure it's activated
   ```

2. **Permission errors on Linux/macOS:**
   ```bash
   # Make scripts executable
   chmod +x *.py
   ```

## Cleaning Up

When you're done with the lab, clean up resources to avoid unnecessary charges:

1. Stop the Docker containers:

```bash
docker-compose down
```

2. Delete the Kinesis streams:

```bash
aws kinesis delete-stream --stream-name data-stream
aws kinesis delete-stream --stream-name analytics
```

3. Verify the streams have been deleted:

```bash
aws kinesis list-streams
```

## Understanding the Lab Files

- `docker-compose.yml` - Docker configuration for Kafka
- `requirements.txt` - Python dependencies
- `config.py` - Configuration for Kafka and Kinesis
- `kafka_examples.py` - Examples for Kafka producer, consumer, and analytics
- `kinesis_examples.py` - Examples for Kinesis producer, consumer, and analytics
- `kafka_to_kinesis.py` - Example showing how to bridge Kafka to Kinesis

## Additional Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Amazon Kinesis Documentation](https://docs.aws.amazon.com/kinesis/index.html)
- [kafka-python Documentation](https://kafka-python.readthedocs.io/)
- [boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [Docker Documentation](https://docs.docker.com/)
- [LocalStack Documentation](https://docs.localstack.cloud/)