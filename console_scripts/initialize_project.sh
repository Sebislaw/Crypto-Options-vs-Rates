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

# 8. Deploy NiFi Flow
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

# 9. Setup Batch Layer Cron Job
echo "=================================================="
echo "   Setting up Batch Analytics Cron Job"
echo "=================================================="
BATCH_CRON_SCRIPT="$PROJECT_ROOT/console_scripts/setup_batch_cron.sh"

if [ -f "$BATCH_CRON_SCRIPT" ]; then
    echo "   -> Installing cron job for automated batch processing..."
    bash "$BATCH_CRON_SCRIPT" install
    echo ""
    echo "   Batch analytics will run every 6 hours automatically."
    echo "   Use 'bash $BATCH_CRON_SCRIPT status' to check cron status."
else
    echo "   [!] WARNING: Batch cron setup script not found at $BATCH_CRON_SCRIPT"
    echo "       You will need to run batch jobs manually."
fi



echo "=================================================="
echo "   Initialization Complete!"
echo "=================================================="
echo ""
echo "Available Commands:"
echo "  - Start ingestion:  bash console_scripts/start_ingestion.sh"
echo "  - Stop ingestion:   bash console_scripts/stop_ingestion.sh"
echo "  - Batch cron status: bash console_scripts/setup_batch_cron.sh status"
echo "  - View batch logs:   bash console_scripts/setup_batch_cron.sh logs"
echo "  - Run batch manually: bash batch_layer/run_batch_analytics.sh"
echo ""
echo "=================================================="