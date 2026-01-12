#!/bin/bash

# ==============================================================================
# Script Name: stop_ingestion.sh
# Description: Stops the Python collectors.
# ==============================================================================

PROJECT_ROOT="$HOME/Crypto-Options-vs-Rates"
PID_DIR="$PROJECT_ROOT/pids"

echo "=================================================="
echo "   Stopping Crypto Ingestion Collectors"
echo "=================================================="

# Function to kill process by PID file
kill_process() {
    SERVICE_NAME=$1
    PID_FILE="$PID_DIR/$SERVICE_NAME.pid"
    
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p $PID > /dev/null; then
            echo "Stopping $SERVICE_NAME (PID: $PID)..."
            kill $PID
            rm "$PID_FILE"
            echo "   -> Stopped."
        else
            echo "$SERVICE_NAME (PID: $PID) is not running. Removing stale PID file."
            rm "$PID_FILE"
        fi
    else
        echo "$SERVICE_NAME is not running (No PID file)."
    fi
}

# 1. Stop Python Collectors
kill_process "binance"
kill_process "polymarket_clob"

# 2. Stop Spark Streaming
kill_process "spark_streaming"

# 3. Force kill any lingering Spark processes (Safety net)
echo "Ensuring all Spark Submit processes are dead..."
pkill -f "spark_streaming.py" 2>/dev/null

echo "=================================================="
echo "   Collectors Stopped"
echo "=================================================="