#!/bin/bash

# ==============================================================================
# Script Name: run_functional_tests.sh
# Description: Runs all functional tests for the Crypto Options vs Rates project
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

echo "=================================================="
echo "   Running Functional Tests"
echo "=================================================="

# Check if we're in a conda environment
if [ -z "$CONDA_DEFAULT_ENV" ]; then
    echo "[INFO] Attempting to activate conda environment..."
    if [ -f ~/miniconda/bin/activate ]; then
        source ~/miniconda/bin/activate crypto_project 2>/dev/null
    elif [ -f ~/anaconda3/bin/activate ]; then
        source ~/anaconda3/bin/activate crypto_project 2>/dev/null
    fi
fi

echo "[INFO] Python: $(which python)"
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
