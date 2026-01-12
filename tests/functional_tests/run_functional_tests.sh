#!/bin/bash

# ==============================================================================
# Script Name: run_functional_tests.sh
# Description: Runs all functional tests for the Crypto Options vs Rates project
# Requirements: Must be run on the VM with HBase running
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

echo "=================================================="
echo "   Running Functional Tests"
echo "=================================================="

# ------------------------------------------------------------------------------
# 1. Activate Conda Environment
# ------------------------------------------------------------------------------
# The VM uses miniconda3 with an environment called 'venv'
MINICONDA_DIR="$HOME/miniconda3"
ENV_NAME="venv"

if [ -z "$CONDA_DEFAULT_ENV" ]; then
    echo "[INFO] Activating conda environment '$ENV_NAME'..."
    
    if [ -f "$MINICONDA_DIR/etc/profile.d/conda.sh" ]; then
        source "$MINICONDA_DIR/etc/profile.d/conda.sh"
        conda activate "$ENV_NAME"
    elif [ -f "$MINICONDA_DIR/bin/activate" ]; then
        source "$MINICONDA_DIR/bin/activate" "$ENV_NAME"
    else
        echo "[!] ERROR: Conda not found at $MINICONDA_DIR"
        echo "    Run console_scripts/initialize_project.sh first."
        exit 1
    fi
fi

# Verify we're in an environment
if [ -z "$CONDA_DEFAULT_ENV" ]; then
    echo "[!] WARNING: Could not activate conda environment."
    echo "    Tests may fail if dependencies are not installed."
fi

# ------------------------------------------------------------------------------
# 2. Check/Install Test Dependencies
# ------------------------------------------------------------------------------
echo "[INFO] Checking test dependencies..."

# Check for pytest
if ! python -c "import pytest" 2>/dev/null; then
    echo "[INFO] Installing pytest..."
    pip install -q pytest>=8.0.0
fi

# Check for happybase
if ! python -c "import happybase" 2>/dev/null; then
    echo "[INFO] Installing happybase..."
    pip install -q happybase>=1.2.0
fi

echo "[INFO] Python: $(which python)"
echo "[INFO] Environment: ${CONDA_DEFAULT_ENV:-system}"
echo "[INFO] Working directory: $PROJECT_ROOT"
echo ""

# Change to project root
cd "$PROJECT_ROOT"

# Create evidence directory if it doesn't exist
mkdir -p "$SCRIPT_DIR/evidence"

# Timestamp for this test run
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
EVIDENCE_FILE="$SCRIPT_DIR/evidence/test_run_$TIMESTAMP.log"

# Run tests
echo "--------------------------------------------------"
echo "Running HBase Setup Tests..."
echo "--------------------------------------------------"
python -m pytest tests/functional_tests/test_hbase_setup.py -v 2>&1 | tee -a "$EVIDENCE_FILE"
HBASE_RESULT=${PIPESTATUS[0]}

echo ""
echo "--------------------------------------------------"
echo "Running Data Integrity Tests..."
echo "--------------------------------------------------"
python -m pytest tests/functional_tests/test_data_integrity.py -v 2>&1 | tee -a "$EVIDENCE_FILE"
INTEGRITY_RESULT=${PIPESTATUS[0]}

echo ""
echo "=================================================="
echo "   Test Summary"
echo "=================================================="

if [ $HBASE_RESULT -eq 0 ]; then
    echo "✓ HBase Setup Tests: PASSED"
else
    echo "✗ HBase Setup Tests: FAILED"
fi

if [ $INTEGRITY_RESULT -eq 0 ]; then
    echo "✓ Data Integrity Tests: PASSED"
else
    echo "✗ Data Integrity Tests: FAILED"
fi

echo ""
echo "Evidence saved to: $EVIDENCE_FILE"
echo "=================================================="

# Exit with appropriate code
if [ $HBASE_RESULT -eq 0 ] && [ $INTEGRITY_RESULT -eq 0 ]; then
    exit 0
else
    exit 1
fi
