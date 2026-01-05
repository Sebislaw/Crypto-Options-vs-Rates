#!/bin/bash

# ==============================================================================
# Script Name: setup_conda.sh
# Description: Installs Miniconda 4.12.0 and sets up Python 3.10 env.
# ==============================================================================

PROJECT_ROOT="$HOME/Crypto-Options-vs-Rates"
REQUIREMENTS_FILE="$PROJECT_ROOT/requirements/python_requirements.txt"
MINICONDA_DIR="$HOME/miniconda3"
ENV_NAME="venv"

echo "=================================================="
echo "   Setting up Isolated Python 3.10 (Conda)"
echo "=================================================="

# 1. Install Miniconda (Old-GLIBC Compatible Version)
if [ ! -d "$MINICONDA_DIR" ]; then
    echo "   -> Downloading Miniconda 4.12.0..."
    wget -q https://repo.anaconda.com/miniconda/Miniconda3-py39_4.12.0-Linux-x86_64.sh -O miniconda.sh
    bash miniconda.sh -b -p "$MINICONDA_DIR"
    rm miniconda.sh
    
    echo "   -> Configuring Conda to be silent (no (base) prompt)..."
    "$MINICONDA_DIR/bin/conda" config --set auto_activate_base false
    "$MINICONDA_DIR/bin/conda" init bash
fi

# 2. Create Python 3.10 Environment
if [ ! -d "$MINICONDA_DIR/envs/$ENV_NAME" ]; then
    echo "   -> Creating environment '$ENV_NAME' with Python 3.10..."
    "$MINICONDA_DIR/bin/conda" create -y -n "$ENV_NAME" python=3.10
else
    echo "   -> Environment exists. Ensuring Python 3.10..."
    "$MINICONDA_DIR/bin/conda" install -y -n "$ENV_NAME" python=3.10
fi

# 3. Install Requirements via direct path (prevents shell activation issues)
if [ -f "$REQUIREMENTS_FILE" ]; then
    echo "   -> Installing dependencies..."
    "$MINICONDA_DIR/envs/$ENV_NAME/bin/pip" install --upgrade pip
    "$MINICONDA_DIR/envs/$ENV_NAME/bin/pip" install -r "$REQUIREMENTS_FILE"
    echo "   -> Dependencies installed successfully."
else
    echo "   [!] WARNING: Requirements file not found at $REQUIREMENTS_FILE"
fi

echo "=================================================="
echo "   Conda Environment Ready at $ENV_NAME"
echo "=================================================="