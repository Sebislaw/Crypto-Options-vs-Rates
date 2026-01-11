#!/bin/bash

# ============================================================================
# HBase Table Creation Script
# Purpose: Create HBase table with proper column families for analytics
# ============================================================================

echo "============================================================"
echo "  Creating HBase Table for Market Analytics"
echo "============================================================"

# Check if HBase shell is available
if ! command -v hbase &> /dev/null; then
    echo "[ERROR] HBase command not found. Make sure HBase is installed and in PATH."
    exit 1
fi

# Create table using HBase shell
hbase shell << 'EOF'

# Disable and drop if exists (for fresh start - comment out in production)
disable 'market_analytics'
drop 'market_analytics'

# Create table with three column families
create 'market_analytics', 
  {NAME => 'price_data', VERSIONS => 1, COMPRESSION => 'GZ'},
  {NAME => 'bet_data', VERSIONS => 1, COMPRESSION => 'GZ'},
  {NAME => 'analysis', VERSIONS => 1, COMPRESSION => 'GZ'}
describe 'market_analytics'

# Show table list
list

puts ""
puts "Table 'market_analytics' created successfully!"
puts ""
puts "Column Families:"
puts "  - price_data:  Binance OHLC data"
puts "  - bet_data:    Polymarket probability metrics"
puts "  - analysis:    Computed correlation results"
puts ""

exit
EOF

echo ""
echo "============================================================"
echo "  HBase Table Setup Complete"
echo "============================================================"
