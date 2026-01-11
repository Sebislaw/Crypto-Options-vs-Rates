#!/bin/bash

# ==============================================================================
# Script Name: start_ingestion.sh
# Description: Starts Binance, Polymarket, AND Spark Streaming.
# ==============================================================================

# --- Configuration ---
PROJECT_ROOT="$HOME/Crypto-Options-vs-Rates"
ENV_PYTHON="$HOME/miniconda3/envs/venv/bin/python"
LOG_DIR="$PROJECT_ROOT/logs"
PID_DIR="$PROJECT_ROOT/pids"

# Create directories
mkdir -p "$LOG_DIR" "$PID_DIR"

echo "=================================================="
echo "   Starting Ingestion & Speed Layer"
echo "=================================================="

# 1. Verify Python Environment
if [ ! -f "$ENV_PYTHON" ]; then
    echo "   [!] Error: Python environment not found at $ENV_PYTHON"
    exit 1
fi

# 2. Check HDFS Status
echo "[CHECK] Verifying Infrastructure status..."
if hdfs dfsadmin -report > /dev/null 2>&1; then
    echo "   -> HDFS is running and reachable."
else
    echo "   [!] WARNING: HDFS seems unreachable. Collectors may fail to write data."
fi

# 3. Start Binance Collector
echo "[INGEST] Starting Binance Collector..."
cd "$PROJECT_ROOT" || exit
export PYTHONPATH=$PROJECT_ROOT
nohup "$ENV_PYTHON" ingestion_layer/binance/main.py >> "$LOG_DIR/binance_app.log" 2> "$LOG_DIR/binance_error.log" &
echo $! > "$PID_DIR/binance.pid"
echo "   -> Binance Collector running (PID: $(cat $PID_DIR/binance.pid))"

# 4. Start Polymarket WebSocket Collector
echo "[INGEST] Starting Polymarket WebSocket..."
nohup "$ENV_PYTHON" -u ingestion_layer/polymarket/polymarket_clob.py >> "$LOG_DIR/polymarket_app.log" 2> "$LOG_DIR/polymarket_error.log" &
echo $! > "$PID_DIR/polymarket_clob.pid"
echo "   -> Polymarket CLOB Collector running (PID: $(cat $PID_DIR/polymarket_clob.pid))"

# 5. Start Spark Streaming
echo "[SPEED] Starting Spark Speed Layer..."
nohup spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
    "$PROJECT_ROOT/speed_layer/spark/spark_streaming.py" \
    > "$LOG_DIR/spark_speed_layer.log" 2>&1 &

echo $! > "$PID_DIR/spark_streaming.pid"
echo "   -> Spark Job running (PID: $(cat $PID_DIR/spark_streaming.pid))"

echo "=================================================="
echo "   All Systems Started!"
echo "   Logs at: $LOG_DIR"
echo "=================================================="