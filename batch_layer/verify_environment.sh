#!/bin/bash

# ============================================================================
# Batch Layer Environment Verification Script
# Purpose: Check all prerequisites before running batch analytics
# ============================================================================

echo "============================================================"
echo "  Batch Layer Environment Verification"
echo "============================================================"
echo ""

ERRORS=0
WARNINGS=0

# Function to print status
check_ok() {
    echo "  ✓ $1"
}

check_warn() {
    echo "  ⚠ $1"
    ((WARNINGS++))
}

check_error() {
    echo "  ✗ $1"
    ((ERRORS++))
}

# Check 1: HDFS
echo "[1/8] Checking HDFS..."
if hdfs dfsadmin -report > /dev/null 2>&1; then
    check_ok "HDFS is running"
    
    # Check if cleansed data exists
    if hdfs dfs -test -d /user/vagrant/cleansed/binance; then
        BINANCE_FILES=$(hdfs dfs -count /user/vagrant/cleansed/binance | awk '{print $2}')
        if [ "$BINANCE_FILES" -gt 0 ]; then
            check_ok "Binance data exists ($BINANCE_FILES partitions)"
        else
            check_warn "Binance directory exists but is empty"
        fi
    else
        check_error "Binance data directory not found"
    fi
    
    if hdfs dfs -test -d /user/vagrant/cleansed/polymarket_trade; then
        POLY_FILES=$(hdfs dfs -count /user/vagrant/cleansed/polymarket_trade | awk '{print $2}')
        if [ "$POLY_FILES" -gt 0 ]; then
            check_ok "Polymarket data exists ($POLY_FILES partitions)"
        else
            check_warn "Polymarket directory exists but is empty"
        fi
    else
        check_error "Polymarket data directory not found"
    fi
else
    check_error "HDFS is not accessible"
fi
echo ""

# Check 2: Hive
echo "[2/8] Checking Hive..."
if hive -e "SHOW DATABASES;" > /dev/null 2>&1; then
    check_ok "Hive is accessible"
    
    # Check if tables exist
    if hive -e "SHOW TABLES;" 2>/dev/null | grep -q "binance_klines"; then
        check_ok "Table 'binance_klines' exists"
        BINANCE_ROWS=$(hive -e "SELECT COUNT(*) FROM binance_klines;" 2>/dev/null | tail -1)
        if [ "$BINANCE_ROWS" -gt 0 ] 2>/dev/null; then
            check_ok "Binance table has $BINANCE_ROWS rows"
        else
            check_warn "Binance table exists but has no data"
        fi
    else
        check_error "Table 'binance_klines' not found"
    fi
    
    if hive -e "SHOW TABLES;" 2>/dev/null | grep -q "polymarket_orderbook"; then
        check_ok "Table 'polymarket_orderbook' exists"
        POLY_ROWS=$(hive -e "SELECT COUNT(*) FROM polymarket_orderbook;" 2>/dev/null | tail -1)
        if [ "$POLY_ROWS" -gt 0 ] 2>/dev/null; then
            check_ok "Polymarket table has $POLY_ROWS rows"
        else
            check_warn "Polymarket table exists but has no data"
        fi
    else
        check_error "Table 'polymarket_orderbook' not found"
    fi
else
    check_error "Hive is not accessible"
fi
echo ""

# Check 3: HBase
echo "[3/8] Checking HBase..."
if echo "status" | hbase shell -n > /dev/null 2>&1; then
    check_ok "HBase is running"
    
    # Check if table exists
    if echo "exists 'market_analytics'" | hbase shell -n 2>&1 | grep -q "does exist"; then
        check_ok "Table 'market_analytics' exists"
    else
        check_warn "Table 'market_analytics' not found (will be created)"
    fi
    
    # Check Thrift server
    if ps aux | grep -v grep | grep -q "ThriftServer"; then
        check_ok "HBase Thrift server is running"
    else
        check_warn "HBase Thrift server not detected (needed for Spark writes)"
        echo "         Start with: hbase-daemon.sh start thrift"
    fi
else
    check_error "HBase is not accessible"
fi
echo ""

# Check 4: Spark
echo "[4/8] Checking Spark..."
if command -v spark-submit &> /dev/null; then
    SPARK_VERSION=$(spark-submit --version 2>&1 | grep "version" | head -1)
    check_ok "Spark is installed: $SPARK_VERSION"
else
    check_error "spark-submit not found in PATH"
fi
echo ""

# Check 5: Python Dependencies
echo "[5/8] Checking Python dependencies..."
if python -c "import pyspark" 2>/dev/null; then
    check_ok "PySpark is available"
else
    check_warn "PySpark not found in Python path"
fi

if python -c "import happybase" 2>/dev/null; then
    check_ok "happybase is installed"
else
    check_warn "happybase not installed (needed for HBase writes)"
    echo "         Install with: pip install happybase"
fi

if python -c "import pandas" 2>/dev/null; then
    check_ok "pandas is installed"
else
    check_warn "pandas not installed"
fi
echo ""

# Check 6: Batch Layer Files
echo "[6/8] Checking batch layer files..."
PROJECT_ROOT="$HOME/Crypto-Options-vs-Rates"

if [ -f "$PROJECT_ROOT/batch_layer/hive/create_tables.sql" ]; then
    check_ok "Hive DDL scripts exist"
else
    check_error "Hive DDL scripts not found"
fi

if [ -f "$PROJECT_ROOT/batch_layer/spark/batch_analytics.py" ]; then
    check_ok "Spark job script exists"
else
    check_error "Spark job script not found"
fi

if [ -f "$PROJECT_ROOT/batch_layer/run_batch_analytics.sh" ]; then
    check_ok "Execution script exists"
    if [ -x "$PROJECT_ROOT/batch_layer/run_batch_analytics.sh" ]; then
        check_ok "Execution script is executable"
    else
        check_warn "Execution script not executable (run: chmod +x batch_layer/run_batch_analytics.sh)"
    fi
else
    check_error "Execution script not found"
fi
echo ""

# Check 7: Ports
echo "[7/8] Checking service ports..."
if nc -z localhost 9083 2>/dev/null; then
    check_ok "Hive Metastore (9083) is accessible"
else
    check_warn "Hive Metastore port 9083 not accessible"
fi

if nc -z localhost 9090 2>/dev/null; then
    check_ok "HBase Thrift (9090) is accessible"
else
    check_warn "HBase Thrift port 9090 not accessible"
fi

if nc -z localhost 50070 2>/dev/null; then
    check_ok "HDFS NameNode (50070) is accessible"
else
    check_warn "HDFS NameNode port 50070 not accessible"
fi
echo ""

# Check 8: Disk Space
echo "[8/8] Checking disk space..."
DISK_USAGE=$(df -h / | awk 'NR==2 {print $5}' | sed 's/%//')
if [ "$DISK_USAGE" -lt 80 ]; then
    check_ok "Sufficient disk space (${DISK_USAGE}% used)"
elif [ "$DISK_USAGE" -lt 90 ]; then
    check_warn "Disk space running low (${DISK_USAGE}% used)"
else
    check_error "Critically low disk space (${DISK_USAGE}% used)"
fi
echo ""

# Summary
echo "============================================================"
echo "  Verification Summary"
echo "============================================================"
echo ""
echo "  Errors:   $ERRORS"
echo "  Warnings: $WARNINGS"
echo ""

if [ $ERRORS -eq 0 ] && [ $WARNINGS -eq 0 ]; then
    echo "✓ All checks passed! You're ready to run the batch analytics."
    echo ""
    echo "Next steps:"
    echo "  cd ~/Crypto-Options-vs-Rates"
    echo "  batch_layer/run_batch_analytics.sh"
    exit 0
elif [ $ERRORS -eq 0 ]; then
    echo "⚠ System is functional but has warnings."
    echo "  Review warnings above and consider fixing them."
    echo ""
    echo "You can still try running:"
    echo "  batch_layer/run_batch_analytics.sh"
    exit 0
else
    echo "✗ Critical errors detected. Fix the errors above before proceeding."
    echo ""
    echo "Common fixes:"
    echo "  - Start services: sudo /home/vagrant/scripts/bootstrap.sh"
    echo "  - Create Hive tables: hive -f batch_layer/hive/create_tables.sql"
    echo "  - Repair partitions: hive -f batch_layer/hive/repair_partitions.sql"
    echo "  - Create HBase table: bash batch_layer/hbase/create_table.sh"
    exit 1
fi
