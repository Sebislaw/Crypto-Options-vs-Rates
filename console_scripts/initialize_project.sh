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
if [ -d "$PROJECT_ROOT/console_scripts" ]; then
    find "$PROJECT_ROOT/console_scripts" -name "*.sh" -type f -exec sed -i 's/\r$//' {} +
    find "$PROJECT_ROOT/console_scripts" -name "*.sh" -type f -exec chmod +x {} +
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

echo "=================================================="
echo "   Initialization Complete!"
echo "   Run 'console_scripts/start_ingestion.sh' to start collectors."
echo "=================================================="