-- ============================================================================
-- Partition Repair Script
-- Purpose: Discover and register existing date partitions in HDFS
-- Run this after creating tables to load existing data
-- ============================================================================

-- Repair Binance table partitions
MSCK REPAIR TABLE binance_klines;

-- Repair Polymarket table partitions
MSCK REPAIR TABLE polymarket_orderbook;

-- Verify partitions were loaded
SHOW PARTITIONS binance_klines;
SHOW PARTITIONS polymarket_orderbook;

-- Quick data check (commented out - use separate queries if needed)
-- SELECT 'Binance row count' as table_name, COUNT(*) as `rows` FROM binance_klines
-- UNION ALL
-- SELECT 'Polymarket row count' as table_name, COUNT(*) as `rows` FROM polymarket_orderbook;
