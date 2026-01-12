#!/bin/bash
# Quick verification script to check if files have the correct content

echo "Checking file synchronization..."
echo ""

echo "1. Checking Hive DDL for backticks around 'timestamp':"
if grep -q '`timestamp`' batch_layer/hive/create_tables.sql; then
    echo "   ✓ CORRECT: Found backticks around timestamp"
else
    echo "   ✗ INCORRECT: No backticks found"
    echo "   Showing first column definition:"
    head -20 batch_layer/hive/create_tables.sql | grep -A 1 "binance_klines"
fi
echo ""

echo "2. Checking HBase compression (should be GZ, not SNAPPY):"
if grep -q "COMPRESSION => 'GZ'" batch_layer/hbase/create_table.sh; then
    echo "   ✓ CORRECT: Using GZ compression"
elif grep -q "COMPRESSION => 'SNAPPY'" batch_layer/hbase/create_table.sh; then
    echo "   ✗ INCORRECT: Still using SNAPPY"
fi
echo ""

echo "3. Checking Spark Hive metastore config:"
if grep -q "spark.sql.hive.metastore.version" batch_layer/spark/batch_analytics.py; then
    echo "   ✓ CORRECT: Found metastore version config"
    grep "metastore" batch_layer/spark/batch_analytics.py | head -2
else
    echo "   ✗ INCORRECT: Missing metastore version config"
fi
echo ""

echo "File sync verification complete."
