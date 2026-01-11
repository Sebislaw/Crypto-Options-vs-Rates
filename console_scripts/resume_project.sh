#!/bin/bash

# ==============================================================================
# Script Name: resume_project.sh
# Description: Restarts the underlying infrastructure (HDFS, YARN, Kafka)
#              Run this ONCE every time you turn on the VM.
# ==============================================================================

echo "=================================================="
echo "   RESUMING INFRASTRUCTURE (HDFS, YARN, KAFKA)"
echo "=================================================="

# 1. Start System Services (HDFS, YARN)
echo "[1/3] Bootstrapping Hadoop Stack..."
sudo /home/vagrant/scripts/bootstrap.sh

# 2. Start Speed Layer Infrastructure (Zookeeper & Kafka)
echo "[2/3] Starting Kafka Broker..."

# Stop any stale processes just in case
sudo /usr/local/kafka/bin/kafka-server-stop.sh 2>/dev/null
sudo /usr/local/kafka/bin/zookeeper-server-stop.sh 2>/dev/null
sleep 2

# Start Zookeeper first
echo "   -> Starting Zookeeper..."
sudo /usr/local/kafka/bin/zookeeper-server-start.sh -daemon /usr/local/kafka/config/zookeeper.properties
echo "      Waiting 10s for Zookeeper..."
sleep 10

# Start Kafka
echo "   -> Starting Kafka Broker..."
sudo /usr/local/kafka/bin/kafka-server-start.sh -daemon /usr/local/kafka/config/server.properties
echo "      Waiting 15s for Kafka..."
sleep 15

# 3. Check Status
echo "[3/3] Verifying Infrastructure..."
jps

echo "=================================================="
echo "   INFRASTRUCTURE READY."
echo "=================================================="