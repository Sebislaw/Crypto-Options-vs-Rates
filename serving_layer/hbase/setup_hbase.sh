#!/bin/bash

# ==============================================================================
# Script Name: setup_hbase.sh
# Description: Creates HBase tables for the serving layer (Lambda Architecture)
# Tables:
#   - market_analytics: Batch layer output (historical 15-min window analytics)
#   - market_live: Speed layer output (real-time streaming data)
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"

echo "=================================================="
echo "   Setting up HBase Tables (Serving Layer)"
echo "=================================================="

# ------------------------------------------------------------------------------
# 1. Check if HBase is running
# ------------------------------------------------------------------------------
echo "[CHECK] Verifying HBase status..."

# Check HBase Master port
if netstat -tuln 2>/dev/null | grep ":16010 " > /dev/null; then
    echo "   -> HBase Master is running on port 16010."
else
    echo "   [!] WARNING: HBase Master does not appear to be running on port 16010."
    echo "       Attempting to continue anyway..."
fi

# Check if hbase command is available
if ! command -v hbase &> /dev/null; then
    echo "   [!] ERROR: 'hbase' command not found in PATH."
    echo "       Please ensure HBase is installed and configured."
    exit 1
fi

# ------------------------------------------------------------------------------
# 1b. Ensure HBase Thrift server is running (required for HappyBase)
# ------------------------------------------------------------------------------
echo "[CHECK] Verifying HBase Thrift server..."

if netstat -tuln 2>/dev/null | grep ":9090 " > /dev/null; then
    echo "   -> HBase Thrift server is running on port 9090."
else
    echo "   -> HBase Thrift server is NOT running. Starting it..."
    hbase-daemon.sh start thrift
    sleep 3
    if netstat -tuln 2>/dev/null | grep ":9090 " > /dev/null; then
        echo "   -> Thrift server started successfully on port 9090."
    else
        echo "   [!] WARNING: Thrift server may not have started. HappyBase connections may fail."
    fi
fi

# ------------------------------------------------------------------------------
# 2. Function to create table if it doesn't exist
# ------------------------------------------------------------------------------
create_table_if_not_exists() {
    TABLE_NAME=$1
    COLUMN_FAMILIES=$2
    
    # Check if table exists
    EXISTS=$(echo "exists '$TABLE_NAME'" | hbase shell 2>/dev/null | grep -c "true")
    
    if [ "$EXISTS" -gt 0 ]; then
        echo "   [INFO] Table '$TABLE_NAME' already exists."
    else
        echo "   [CREATING] Table '$TABLE_NAME'..."
        echo "create '$TABLE_NAME', $COLUMN_FAMILIES" | hbase shell 2>/dev/null
        
        # Verify creation
        VERIFY=$(echo "exists '$TABLE_NAME'" | hbase shell 2>/dev/null | grep -c "true")
        if [ "$VERIFY" -gt 0 ]; then
            echo "   [SUCCESS] Table '$TABLE_NAME' created successfully."
        else
            echo "   [!] ERROR: Failed to create table '$TABLE_NAME'."
            return 1
        fi
    fi
}

# ------------------------------------------------------------------------------
# 3. Create Tables
# ------------------------------------------------------------------------------
echo "--------------------------------------------------"
echo "Creating HBase Tables..."
echo "--------------------------------------------------"

# Market Analytics Table (Batch Layer)
# Column Families: price_data, bet_data, analysis (aligned with batch_layer)
# Using GZ compression to match create_table.sh
create_table_if_not_exists "market_analytics" \
    "{NAME => 'price_data', VERSIONS => 1, COMPRESSION => 'GZ', TTL => 31536000}, {NAME => 'bet_data', VERSIONS => 1, COMPRESSION => 'GZ', TTL => 31536000}, {NAME => 'analysis', VERSIONS => 1, COMPRESSION => 'GZ', TTL => 31536000}"

# Market Live Table (Speed Layer)
# Column Family: d (compact name for live data)
# 1-day TTL (86400 seconds) - only recent data needed
# Using GZ compression
create_table_if_not_exists "market_live" \
    "{NAME => 'd', VERSIONS => 1, COMPRESSION => 'GZ', TTL => 86400}"

# ------------------------------------------------------------------------------
# 4. Verify Tables
# ------------------------------------------------------------------------------
echo "--------------------------------------------------"
echo "Verification: Listing HBase Tables..."
echo "--------------------------------------------------"

echo "list" | hbase shell 2>/dev/null | grep -E "(market_analytics|market_live|TABLE)"

echo "=================================================="
echo "   HBase Setup Complete!"
echo "=================================================="
echo ""
echo "Tables created:"
echo "  - market_analytics (Batch Layer: price_data, bet_data, analysis)"
echo "  - market_live (Speed Layer: d)"
echo ""
echo "To verify, run: hbase shell"
echo "  > describe 'market_analytics'"
echo "  > describe 'market_live'"
echo "=================================================="
