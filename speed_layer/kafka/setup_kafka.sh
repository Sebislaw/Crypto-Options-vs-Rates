#!/bin/bash

# ==============================================================================
# Script Name: setup_kafka.sh
# Description: Deploys and configures Apache Kafka for the Speed Layer.
#              Manages Zookeeper, Kafka Broker, and creates necessary topics
#              for streaming cryptocurrency and prediction market data.
# Location: speed_layer/kafka/setup_kafka.sh
# 
# Topics Created:
#   - binance: Cryptocurrency price and volume data from Binance
#   - polymarket_trade: Trade events from Polymarket prediction markets
#   - polymarket_metadata: Market metadata from Polymarket
#
# Prerequisites:
#   - Apache Kafka installed at /usr/local/kafka
#   - Sufficient permissions to start services
# ==============================================================================

# Kafka Installation Path
KAFKA_HOME="/usr/local/kafka"
KAFKA_BIN="$KAFKA_HOME/bin"
KAFKA_CONFIG="$KAFKA_HOME/config"

echo "=================================================="
echo "Starting Kafka Deployment for Speed Layer"
echo "=================================================="

# 1. Start Zookeeper
# Check if Zookeeper is running on port 2181
if netstat -tuln | grep ":2181 " > /dev/null; then
    echo "[INFO] Zookeeper is already running."
else
    echo "[STARTING] Zookeeper..."
    sudo $KAFKA_BIN/zookeeper-server-start.sh -daemon $KAFKA_CONFIG/zookeeper.properties
    echo "[SUCCESS] Zookeeper started in daemon mode."
    # Wait a moment for Zookeeper to initialize
    sleep 5
fi

# 2. Start Kafka Broker
# Check if Kafka is running on port 9092
if netstat -tuln | grep ":9092 " > /dev/null; then
    echo "[INFO] Kafka Broker is already running."
else
    echo "[STARTING] Kafka Broker..."
    sudo $KAFKA_BIN/kafka-server-start.sh -daemon $KAFKA_CONFIG/server.properties
    echo "[SUCCESS] Kafka Broker started in daemon mode."
    # Wait for Kafka to initialize
    sleep 10
fi

# 3. Create Topics
echo "--------------------------------------------------"
echo "Creating Topics..."
echo "--------------------------------------------------"

# ------------------------------------------------------------------------------
# Function: create_topic
# Description: Creates a Kafka topic if it doesn't already exist.
#              Checks for topic existence before attempting creation to avoid errors.
# Parameters:
#   $1 - TOPIC_NAME: Name of the Kafka topic to create
# Configuration:
#   - Replication Factor: 1 (suitable for single-node setup)
#   - Partitions: 1 (can be increased for higher throughput)
# ------------------------------------------------------------------------------
create_topic() {
    TOPIC_NAME=$1
    # Check if topic exists
    if sudo $KAFKA_BIN/kafka-topics.sh --list --bootstrap-server localhost:9092 | grep -x "$TOPIC_NAME" > /dev/null; then
        echo "[INFO] Topic '$TOPIC_NAME' already exists."
    else
        sudo $KAFKA_BIN/kafka-topics.sh --create \
            --bootstrap-server localhost:9092 \
            --replication-factor 1 \
            --partitions 1 \
            --topic "$TOPIC_NAME"
        echo "[SUCCESS] Topic '$TOPIC_NAME' created."
    fi
}

# Create the 3 required topics
create_topic "binance"
create_topic "polymarket_trade"
create_topic "polymarket_metadata"

echo "=================================================="
echo "Deployment Finished. Current Topics:"
sudo $KAFKA_BIN/kafka-topics.sh --list --bootstrap-server localhost:9092
echo "=================================================="