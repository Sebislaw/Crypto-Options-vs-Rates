#!/bin/bash

# ==============================================================================
# Script Name: initialize_project.sh
# Description: Master setup script. Installs compilers, fixes permissions, triggers Conda.
# ==============================================================================

PROJECT_ROOT="$HOME/Crypto-Options-vs-Rates"
LOG_DIR="$PROJECT_ROOT/logs"
PID_DIR="$PROJECT_ROOT/pids"

echo "=================================================="
echo "   Initializing Crypto Project Environment"
echo "=================================================="

# 1. Clean up project folders and fix executable permissions
mkdir -p "$LOG_DIR" "$PID_DIR"
if [ -d "$PROJECT_ROOT" ]; then
    find "$PROJECT_ROOT" -name "*.sh" -type f -exec sed -i 's/\r$//' {} +
    find "$PROJECT_ROOT" -name "*.sh" -type f -exec chmod +x {} +
fi

# 2. Install System Utilities & Compilers
echo "[SETUP] Installing system utilities and compilers..."
sudo apt-get update -qq
sudo apt-get install -y netcat git curl build-essential

# 3. Setup Isolated Python Environment
CONDA_SETUP_SCRIPT="$PROJECT_ROOT/console_scripts/setup_conda.sh"
if [ -f "$CONDA_SETUP_SCRIPT" ]; then
    bash "$CONDA_SETUP_SCRIPT"
else
    echo "   [!] Error: $CONDA_SETUP_SCRIPT not found."
    exit 1
fi

# 4. Bootstrap Services
echo "=================================================="
echo "   Bootstrapping VM Services"
echo "=================================================="

BOOTSTRAP_SCRIPT="/home/vagrant/scripts/bootstrap.sh"
if [ -f "$BOOTSTRAP_SCRIPT" ]; then
    echo "   -> Starting services with sudo..."
    sudo bash "$BOOTSTRAP_SCRIPT"
else
    echo "   [!] WARNING: Bootstrap script not found at $BOOTSTRAP_SCRIPT"
fi

# 5. Check HDFS Status (Using direct admin report for accuracy)
echo "[CHECK] Verifying Infrastructure status..."
if hdfs dfsadmin -report > /dev/null 2>&1; then
    echo "   -> HDFS is running and reachable."
else
    echo "   [!] WARNING: HDFS seems unreachable. Collectors may fail to write data."
fi

# 6. Create HDFS Directories
echo "[HDFS] Ensuring data directories..."
if command -v hdfs &> /dev/null; then
    hdfs dfs -mkdir -p /user/vagrant/raw/binance 2>/dev/null
    hdfs dfs -mkdir -p /user/vagrant/raw/polymarket_metadata 2>/dev/null
    hdfs dfs -mkdir -p /user/vagrant/raw/polymarket_trade 2>/dev/null
    hdfs dfs -chmod -R 775 /user/vagrant/raw/ 2>/dev/null
fi

# 7. Setup Kafka (speed layer)
echo "=================================================="
echo "   Setting up Kafka (speed_layer)"
echo "=================================================="
KAFKA_SETUP_SCRIPT="$PROJECT_ROOT/speed_layer/kafka/setup_kafka.sh"

if [ -f "$KAFKA_SETUP_SCRIPT" ]; then
    echo "   -> Executing Kafka setup script..."
    bash "$KAFKA_SETUP_SCRIPT"
else
    echo "   [!] WARNING: Kafka setup script not found at $KAFKA_SETUP_SCRIPT"
    echo "       Kafka may not be initialized."
fi

# 8. Deploy NiFi Flow (NEW SECTION)
echo "=================================================="
echo "   Deploying NiFi Configuration"
echo "=================================================="
DEPLOY_NIFI_SCRIPT="$PROJECT_ROOT/ingestion_layer/nifi/setup_nifi.sh"

if [ -f "$DEPLOY_NIFI_SCRIPT" ]; then
    echo "   -> Executing NiFi Auto-Deploy..."
    # Running as bash (not sudo) so it uses the 'vagrant' user for API calls
    bash "$DEPLOY_NIFI_SCRIPT"
else
    echo "   [!] WARNING: NiFi deployment script not found at $DEPLOY_NIFI_SCRIPT"
    echo "       You may need to deploy the template manually."
fi

# 9. Setup HBase Tables (Serving Layer)
echo "=================================================="
echo "   Setting up HBase (serving_layer)"
echo "=================================================="
HBASE_SETUP_SCRIPT="$PROJECT_ROOT/serving_layer/hbase/setup_hbase.sh"

if [ -f "$HBASE_SETUP_SCRIPT" ]; then
    echo "   -> Executing HBase setup script..."
    bash "$HBASE_SETUP_SCRIPT"
else
    echo "   [!] WARNING: HBase setup script not found at $HBASE_SETUP_SCRIPT"
    echo "       HBase tables may not be initialized."
fi



echo "=================================================="
echo "   Initialization Complete!"
echo "=================================================="