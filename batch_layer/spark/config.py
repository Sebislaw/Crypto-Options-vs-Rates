"""
Configuration for Batch Analytics Job
"""

# HBase Configuration
HBASE_HOST = 'localhost'
HBASE_PORT = 9090  # Thrift port
HBASE_TABLE_NAME = 'market_analytics'

# Spark Configuration
APP_NAME = 'CryptoMarketBatchAnalytics'
MASTER = 'local[*]'  # Change to 'yarn' for cluster mode

# Data Sources
BINANCE_TABLE = 'binance_klines'
POLYMARKET_TABLE = 'polymarket_orderbook'

# Time Window Configuration
WINDOW_DURATION = '15 minutes'  # Aggregation window
WATERMARK_DELAY = '1 minute'    # Late data tolerance

# Symbol Mapping: Polymarket market -> Binance symbol
SYMBOL_MAPPING = {
    'btc': 'BTCUSDT',
    'eth': 'ETHUSDT',
    'sol': 'SOLUSDT',
    'xrp': 'XRPUSDT'
}

# Analysis Thresholds
PREDICTION_THRESHOLD = 0.60  # Probability threshold for "bullish" prediction
MIN_PRICE_MOVEMENT = 0.001    # Minimum price change to consider (0.1%)

# HBase Column Families
CF_PRICE = 'price_data'
CF_BET = 'bet_data'
CF_ANALYSIS = 'analysis'
