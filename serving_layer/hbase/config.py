"""
HBase Configuration for Crypto Options vs Rates Project

This module provides centralized configuration for HBase table names,
column families, and connection settings used across the serving layer.
"""

# ==============================================================================
# HBase Connection Settings
# ==============================================================================

HBASE_HOST = 'localhost'
HBASE_PORT = 9090  # Thrift server port (for HappyBase)
HBASE_MASTER_PORT = 16010  # Master web UI port (for health checks)


# ==============================================================================
# Table Names
# ==============================================================================

TABLE_MARKET_ANALYTICS = 'market_analytics'  # Batch layer output
TABLE_MARKET_LIVE = 'market_live'            # Speed layer output


# ==============================================================================
# Column Families
# ==============================================================================

# market_analytics column families (aligned with batch_layer/spark/config.py)
CF_PRICES = 'price_data'       # Binance price data
CF_BETTING = 'bet_data'        # Polymarket betting data
CF_CORRELATION = 'analysis'    # Analytics results

# market_live column families
CF_DATA = 'd'  # Compact name for live data


# ==============================================================================
# Column Qualifiers (within column families)
# ==============================================================================

# price_data:* columns
COL_OPEN = 'open'
COL_CLOSE = 'close'
COL_HIGH = 'high'
COL_LOW = 'low'
COL_VOLUME = 'volume'
COL_VOLATILITY = 'volatility'  # Keep for compatibility

# bet_data:* columns
COL_AVG_PROB = 'avg_prob'
COL_MAX_PROB = 'max_prob'
COL_BET_ACTIVITY = 'activity'

# analysis:* columns
COL_PREDICTION_RESULT = 'result'
COL_PRICE_MOVEMENT = 'price_movement'
COL_ACTUAL_DIRECTION = 'actual_direction'
COL_PREDICTED_DIRECTION = 'predicted_direction'
COL_TIMESTAMP = 'timestamp'
COL_SYMBOL = 'symbol'
COL_CRYPTO = 'crypto'

# d:* columns (live data)
COL_BINANCE_PRICE = 'binance_price'
COL_POLY_LAST_TRADE = 'poly_last_trade'
COL_POLY_BEST_BID = 'poly_best_bid'
COL_POLY_BEST_ASK = 'poly_best_ask'
COL_IMPLIED_PROB = 'implied_prob'


# ==============================================================================
# RowKey Configuration
# ==============================================================================

ROWKEY_SEPARATOR = '#'

# Supported symbols (matching Binance/Polymarket data)
SYMBOLS = ['BTC', 'ETH', 'SOL', 'XRP']

# Reverse timestamp base for speed layer (ensures newest records first)
REVERSE_TIMESTAMP_MAX = 9999999999999  # 13-digit max (year ~2286)


# ==============================================================================
# Utility Functions
# ==============================================================================

def generate_batch_rowkey(symbol: str, window_end_timestamp: int) -> str:
    """
    Generate a RowKey for the market_analytics table (batch layer).
    
    Format: SYMBOL#WINDOW_END_TIMESTAMP
    Example: BTC#1767201300
    
    Args:
        symbol: Cryptocurrency symbol (e.g., 'BTC', 'ETH')
        window_end_timestamp: Unix timestamp of 15-min window end
        
    Returns:
        Formatted RowKey string
    """
    return f"{symbol.upper()}{ROWKEY_SEPARATOR}{window_end_timestamp}"


def generate_live_rowkey(symbol: str, timestamp: int) -> str:
    """
    Generate a RowKey for the market_live table (speed layer).
    
    Format: SYMBOL#REVERSE_TIMESTAMP
    Uses reverse timestamp so newest records sort first in HBase scans.
    
    Args:
        symbol: Cryptocurrency symbol (e.g., 'BTC', 'ETH')
        timestamp: Unix timestamp (milliseconds)
        
    Returns:
        Formatted RowKey string
    """
    reverse_ts = REVERSE_TIMESTAMP_MAX - timestamp
    return f"{symbol.upper()}{ROWKEY_SEPARATOR}{reverse_ts}"


def parse_rowkey(rowkey: str) -> tuple:
    """
    Parse a RowKey into its components.
    
    Args:
        rowkey: HBase RowKey string
        
    Returns:
        Tuple of (symbol, timestamp_component)
    """
    parts = rowkey.split(ROWKEY_SEPARATOR)
    if len(parts) != 2:
        raise ValueError(f"Invalid RowKey format: {rowkey}")
    return parts[0], int(parts[1])


def reverse_timestamp_to_actual(reverse_ts: int) -> int:
    """
    Convert a reverse timestamp back to actual timestamp.
    
    Args:
        reverse_ts: Reverse timestamp value
        
    Returns:
        Actual Unix timestamp (milliseconds)
    """
    return REVERSE_TIMESTAMP_MAX - reverse_ts
