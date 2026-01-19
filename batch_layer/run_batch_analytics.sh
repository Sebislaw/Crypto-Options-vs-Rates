#!/bin/bash

# ============================================================================
# Batch Analytics Execution Script
# Purpose: Run the complete batch processing pipeline
# Usage: ./run_batch_analytics.sh [YYYY-MM-DD]
# ============================================================================

PROJECT_ROOT="$HOME/Crypto-Options-vs-Rates"
BATCH_DIR="$PROJECT_ROOT/batch_layer"
SPARK_SCRIPT="$BATCH_DIR/spark/batch_analytics.py"
LOG_DIR="$PROJECT_ROOT/logs"

# Configure PySpark to use the Conda venv Python (where happybase is installed)
export PYSPARK_PYTHON="$HOME/miniconda3/envs/venv/bin/python"
export PYSPARK_DRIVER_PYTHON="$HOME/miniconda3/envs/venv/bin/python"

# Create log directory
mkdir -p "$LOG_DIR"

DATE_FILTER="${1:-}"  # Optional date argument

echo "============================================================"
echo "  Crypto Market Batch Analytics Pipeline"
echo "============================================================"
echo ""

# Step 1: Verify Prerequisites
echo "[PREREQ] Checking prerequisites..."

# Check if Hive is running
if ! hive -e "SHOW DATABASES;" > /dev/null 2>&1; then
    echo "[ERROR] Hive is not accessible. Start services first:"
    echo "        sudo /home/vagrant/scripts/bootstrap.sh"
    exit 1
fi
echo "  ✓ Hive is running"

# Check if HBase is running
if ! echo "status" | hbase shell -n > /dev/null 2>&1; then
    echo "[ERROR] HBase is not accessible."
    exit 1
fi
echo "  ✓ HBase is running"

# Check if HBase Thrift server is running (required for happybase)
if ! nc -z localhost 9090 2>/dev/null; then
    echo "  ⚠ HBase Thrift server not running on port 9090. Starting..."
    nohup hbase thrift start -p 9090 > /dev/null 2>&1 &
    # Wait for Thrift to start
    for i in {1..10}; do
        if nc -z localhost 9090 2>/dev/null; then
            echo "  ✓ HBase Thrift server started on port 9090"
            break
        fi
        sleep 1
    done
    if ! nc -z localhost 9090 2>/dev/null; then
        echo "[WARNING] Could not start HBase Thrift server. HBase writes may fail."
    fi
else
    echo "  ✓ HBase Thrift server is running on port 9090"
fi

# Check if Spark is available
if ! command -v spark-submit &> /dev/null; then
    echo "[ERROR] spark-submit not found in PATH"
    exit 1
fi
echo "  ✓ Spark is available"

echo ""

# Step 2: Create/Verify Hive Tables (Idempotent - only creates if not exists)
echo "============================================================"
echo "  Step 1: Setting up Hive Tables"
echo "============================================================"

if [ -f "$BATCH_DIR/hive/create_tables.sql" ]; then
    echo "Verifying Hive tables (idempotent - using IF NOT EXISTS)..."
    hive -f "$BATCH_DIR/hive/create_tables.sql" 2>&1 | tee "$LOG_DIR/hive_create_tables.log"
    
    echo ""
    echo "Repairing partitions..."
    hive -f "$BATCH_DIR/hive/repair_partitions.sql" 2>&1 | tee "$LOG_DIR/hive_repair_partitions.log"
else
    echo "[WARN] Hive SQL scripts not found. Skipping table creation."
fi

echo ""

# Step 3: Create/Verify HBase Table (Idempotent - only creates if not exists)
echo "============================================================"
echo "  Step 2: Setting up HBase Table"
echo "============================================================"

# Check if table already exists
TABLE_EXISTS=$(echo "exists 'market_analytics'" | hbase shell -n 2>/dev/null | grep -c "true")

if [ "$TABLE_EXISTS" -eq 1 ]; then
    echo "HBase table 'market_analytics' already exists. Skipping creation."
else
    echo "HBase table 'market_analytics' does not exist. Creating..."
    if [ -f "$BATCH_DIR/hbase/create_table.sh" ]; then
        bash "$BATCH_DIR/hbase/create_table.sh" 2>&1 | tee "$LOG_DIR/hbase_create_table.log"
    else
        echo "[WARN] HBase creation script not found."
        echo "       Creating table manually..."
        
        hbase shell << 'EOF'
create 'market_analytics', 
  {NAME => 'price_data', VERSIONS => 1},
  {NAME => 'bet_data', VERSIONS => 1},
  {NAME => 'analysis', VERSIONS => 1}
EOF
    fi
fi

echo ""

# Step 4: Run Spark Batch Job
echo "============================================================"
echo "  Step 3: Running Spark Batch Analytics"
echo "============================================================"

if [ -n "$DATE_FILTER" ]; then
    echo "Processing data for date: $DATE_FILTER"
else
    echo "Processing all available data"
fi

echo ""

# Submit Spark job
spark-submit \
    --master local[*] \
    --driver-memory 2g \
    --executor-memory 2g \
    --conf spark.sql.warehouse.dir=/user/hive/warehouse \
    --conf spark.sql.hive.metastore.version=2.3.0 \
    --conf spark.sql.hive.metastore.jars=builtin \
    --py-files "$BATCH_DIR/spark/config.py" \
    "$SPARK_SCRIPT" \
    $DATE_FILTER \
    2>&1 | tee "$LOG_DIR/spark_batch_analytics.log"

SPARK_EXIT_CODE=$?

echo ""

# Step 5: Summary
echo "============================================================"
echo "  Pipeline Execution Summary"
echo "============================================================"

if [ $SPARK_EXIT_CODE -eq 0 ]; then
    echo "✓ Batch analytics completed successfully!"
    echo ""
    echo "Check results:"
    echo "  - HDFS:   hdfs dfs -ls /user/vagrant/batch/analytics_results"
    echo "  - HBase:  echo 'scan \"market_analytics\", {LIMIT => 5}' | hbase shell"
    echo "  - Logs:   cat $LOG_DIR/spark_batch_analytics.log"
else
    echo "✗ Batch analytics failed with exit code: $SPARK_EXIT_CODE"
    echo "  Check logs: $LOG_DIR/spark_batch_analytics.log"
    exit $SPARK_EXIT_CODE
fi

echo ""
echo "============================================================"
echo "  Complete"
echo "============================================================"
