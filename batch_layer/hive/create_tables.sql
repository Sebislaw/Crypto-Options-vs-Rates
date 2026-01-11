-- ============================================================================
-- Hive DDL for Batch Layer Tables
-- Purpose: Create external tables pointing to cleansed Parquet data in HDFS
-- ============================================================================

-- ============================================================================
-- 1. BINANCE TABLE (Spot Price Data)
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS binance_klines (
    `timestamp` BIGINT COMMENT 'Candle open time in milliseconds',
    snapshot_time BIGINT COMMENT 'Time when data was captured',
    `open` DOUBLE COMMENT 'Opening price',
    `high` DOUBLE COMMENT 'Highest price in interval',
    `low` DOUBLE COMMENT 'Lowest price in interval',
    `close` DOUBLE COMMENT 'Closing price',
    `volume` DOUBLE COMMENT 'Trading volume',
    close_time BIGINT COMMENT 'Candle close time in milliseconds',
    quote_asset_volume DOUBLE COMMENT 'Quote asset volume',
    trades BIGINT COMMENT 'Number of trades',
    taker_buy_base DOUBLE COMMENT 'Taker buy base asset volume',
    taker_buy_quote DOUBLE COMMENT 'Taker buy quote asset volume',
    symbol STRING COMMENT 'Trading pair (e.g., BTCUSDT)',
    is_closed BOOLEAN COMMENT 'Whether the candle is closed'
)
PARTITIONED BY (`date` DATE COMMENT 'Partition by date')
STORED AS PARQUET
LOCATION '/user/vagrant/cleansed/binance'
TBLPROPERTIES ('parquet.compression'='SNAPPY');


-- ============================================================================
-- 2. POLYMARKET TRADE TABLE (Orderbook Data)
-- ============================================================================
CREATE EXTERNAL TABLE IF NOT EXISTS polymarket_orderbook (
    market STRING COMMENT 'Market identifier (btc/eth/sol/xrp)',
    asset_id STRING COMMENT 'CLOB token ID',
    timestamp STRING COMMENT 'Event timestamp (may be string format)',
    hash STRING COMMENT 'Event hash',
    event_type STRING COMMENT 'Type of event (price_change, book, etc.)',
    last_trade_price STRING COMMENT 'Most recent trade price (probability 0-1)',
    bids ARRAY<STRUCT<price:STRING, size:STRING>> COMMENT 'Bid orders',
    asks ARRAY<STRUCT<price:STRING, size:STRING>> COMMENT 'Ask orders',
    price_changes ARRAY<STRUCT<
        asset_id:STRING,
        price:STRING,
        size:STRING,
        side:STRING,
        hash:STRING,
        best_bid:STRING,
        best_ask:STRING
    >> COMMENT 'Price change events'
)
PARTITIONED BY (date DATE COMMENT 'Partition by date')
STORED AS PARQUET
LOCATION '/user/vagrant/cleansed/polymarket_trade'
TBLPROPERTIES ('parquet.compression'='SNAPPY');


-- ============================================================================
-- Table Statistics (Optional - improves query performance)
-- ============================================================================
-- Run after loading data:
-- ANALYZE TABLE binance_klines PARTITION(date) COMPUTE STATISTICS;
-- ANALYZE TABLE polymarket_orderbook PARTITION(date) COMPUTE STATISTICS;
