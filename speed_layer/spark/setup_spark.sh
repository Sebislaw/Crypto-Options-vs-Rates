#!/bin/bash

# ==============================================================================
# Script Name: setup_spark.sh
# Description: Creates necessary directories for Spark Speed Layer.
# Location: speed_layer/spark/setup_spark.sh
# ==============================================================================

PROJECT_ROOT="$HOME/Crypto-Options-vs-Rates"
CHECKPOINT_DIR="$PROJECT_ROOT/speed_layer/spark/checkpoints"

echo "=================================================="
echo "   Setting up Spark Directories"
echo "=================================================="

# Create checkpoint directory for Structured Streaming state
if [ ! -d "$CHECKPOINT_DIR" ]; then
    mkdir -p "$CHECKPOINT_DIR"
    echo "   -> Created checkpoint directory: $CHECKPOINT_DIR"
else
    echo "   -> Checkpoint directory already exists."
fi

echo "=================================================="
echo "   Spark Setup Complete"
echo "=================================================="