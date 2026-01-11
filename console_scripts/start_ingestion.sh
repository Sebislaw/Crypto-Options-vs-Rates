#!/bin/bash

# ==============================================================================
# Script Name: start_ingestion.sh
# Description: Starts the Python Collectors (Binance & Polymarket).
# ==============================================================================

# --- Configuration ---
PROJECT_ROOT="$HOME/Crypto-Options-vs-Rates"
ENV_PYTHON="$HOME/miniconda3/envs/venv/bin/python"
LOG_DIR="$PROJECT_ROOT/logs"
PID_DIR="$PROJECT_ROOT/pids"

# Create directories
mkdir -p "$LOG_DIR" "$PID_DIR"

echo "=================================================="
echo "   Starting Crypto Ingestion Collectors (HDFS Mode)"
echo "=================================================="

# 1. Verify Python Environment
if [ ! -f "$ENV_PYTHON" ]; then
    echo "   [!] Error: Python environment not found at $ENV_PYTHON"
    exit 1
fi

# 2. Check HDFS Status (Using direct admin report for accuracy)
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

echo "=================================================="
echo "   Collectors Started"
echo "   Logs (Errors only) at: $LOG_DIR"
echo "=================================================="