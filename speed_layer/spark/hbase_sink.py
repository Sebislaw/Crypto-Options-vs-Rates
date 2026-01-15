"""
HBase Sink Module for Speed Layer

Provides foreachBatch writer functions for Spark Structured Streaming
to persist windowed aggregations to HBase market_live table.

Usage in spark_streaming.py:
    from hbase_sink import write_binance_batch, write_poly_books_batch, write_poly_trades_batch
    
    query = binance_agg.writeStream \
        .foreachBatch(write_binance_batch) \
        .start()
"""

import sys
import os

# Add serving layer to path for config import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'serving_layer', 'hbase'))

try:
    import happybase
except ImportError:
    happybase = None
    print("[WARN] HappyBase not installed. HBase writes will be disabled.")

# Import configuration
try:
    from config import (
        HBASE_HOST,
        HBASE_PORT,
        TABLE_MARKET_LIVE,
        CF_BINANCE,
        CF_POLY_BOOKS,
        CF_POLY_TRADES,
        SPEED_ROW_BINANCE,
        SPEED_ROW_POLY_BOOKS,
        SPEED_ROW_POLY_TRADES,
        generate_speed_rowkey,
    )
except ImportError:
    # Fallback defaults if config not available
    HBASE_HOST = 'localhost'
    HBASE_PORT = 9090
    TABLE_MARKET_LIVE = 'market_live'
    CF_BINANCE = 'b'
    CF_POLY_BOOKS = 'p'
    CF_POLY_TRADES = 't'
    SPEED_ROW_BINANCE = 'BIN'
    SPEED_ROW_POLY_BOOKS = 'PB'
    SPEED_ROW_POLY_TRADES = 'PT'
    REVERSE_TIMESTAMP_MAX = 9999999999999
    ROWKEY_SEPARATOR = '#'
    
    def generate_speed_rowkey(row_type, identifier, window_end_ms):
        reverse_ts = REVERSE_TIMESTAMP_MAX - window_end_ms
        return f"{row_type}{ROWKEY_SEPARATOR}{identifier}{ROWKEY_SEPARATOR}{reverse_ts}"


# Global flag to enable/disable HBase writes
HBASE_ENABLED = True


def _get_connection():
    """Get HBase connection. Returns None if unavailable."""
    if not HBASE_ENABLED or happybase is None:
        return None
    try:
        conn = happybase.Connection(HBASE_HOST, HBASE_PORT)
        conn.open()
        return conn
    except Exception as e:
        print(f"[WARN] HBase connection failed: {e}")
        return None


def _extract_window_end_ms(window_struct):
    """
    Extract window end timestamp in milliseconds from Spark window struct.
    
    The window struct has format: {'start': timestamp, 'end': timestamp}
    """
    if window_struct is None:
        return 0
    # Window end is a datetime, convert to epoch ms
    window_end = window_struct.end
    return int(window_end.timestamp() * 1000)


def write_binance_batch(batch_df, batch_id):
    """
    Write Binance aggregation batch to HBase.
    
    Called by Spark foreachBatch for each micro-batch.
    
    Expected columns:
        window, symbol, current_price, current_sentiment, avg_price,
        min_price, max_price, volatility, total_usdt_volume, avg_sentiment, ticks
    """
    # Use count() for Spark 2.x/3.x compatibility (isEmpty() is Spark 3.3+)
    if batch_df.count() == 0:
        return
    
    conn = _get_connection()
    if conn is None:
        print(f"[DEBUG] Binance batch {batch_id}: HBase disabled, skipping write")
        return
    
    try:
        table = conn.table(TABLE_MARKET_LIVE)
        rows_written = 0
        
        for row in batch_df.collect():
            window_end_ms = _extract_window_end_ms(row['window'])
            symbol = row['symbol']
            rowkey = generate_speed_rowkey(SPEED_ROW_BINANCE, symbol, window_end_ms)
            
            # Build data dict
            data = {}
            
            # Map all columns with null checking
            column_map = {
                'current_price': 'current_price',
                'current_sentiment': 'current_sentiment', 
                'avg_price': 'avg_price',
                'min_price': 'min_price',
                'max_price': 'max_price',
                'volatility': 'volatility',
                'total_usdt_volume': 'total_volume',
                'avg_sentiment': 'avg_sentiment',
                'ticks': 'ticks',
            }
            
            for spark_col, hbase_col in column_map.items():
                val = row[spark_col] if spark_col in row.asDict() else None
                if val is not None:
                    data[f'{CF_BINANCE}:{hbase_col}'.encode()] = str(val).encode()
            
            # Add window timestamp
            data[f'{CF_BINANCE}:window_end'.encode()] = str(window_end_ms).encode()
            
            if data:
                table.put(rowkey.encode(), data)
                rows_written += 1
        
        print(f"[HBASE] Binance batch {batch_id}: wrote {rows_written} rows")
        
    except Exception as e:
        print(f"[ERROR] Binance HBase write failed: {e}")
    finally:
        conn.close()


def write_poly_books_batch(batch_df, batch_id):
    """
    Write Polymarket order book aggregation batch to HBase.
    
    Expected columns:
        window, market_id, current_prob, current_spread, current_imbalance,
        avg_prob, min_prob, max_prob, avg_imbalance, avg_spread, num_updates
    """
    # Use count() for Spark 2.x/3.x compatibility (isEmpty() is Spark 3.3+)
    if batch_df.count() == 0:
        return
    
    conn = _get_connection()
    if conn is None:
        print(f"[DEBUG] Poly books batch {batch_id}: HBase disabled, skipping write")
        return
    
    try:
        table = conn.table(TABLE_MARKET_LIVE)
        rows_written = 0
        
        for row in batch_df.collect():
            window_end_ms = _extract_window_end_ms(row['window'])
            market_id = row['market_id']
            
            # Truncate market_id for rowkey (first 16 chars) to keep it manageable
            market_id_short = market_id[:16] if len(market_id) > 16 else market_id
            rowkey = generate_speed_rowkey(SPEED_ROW_POLY_BOOKS, market_id_short, window_end_ms)
            
            data = {}
            
            column_map = {
                'current_prob': 'current_prob',
                'current_spread': 'current_spread',
                'current_imbalance': 'current_imbalance',
                'avg_prob': 'avg_prob',
                'min_prob': 'min_prob',
                'max_prob': 'max_prob',
                'avg_imbalance': 'avg_imbalance',
                'avg_spread': 'avg_spread',
                'num_updates': 'num_updates',
            }
            
            for spark_col, hbase_col in column_map.items():
                val = row[spark_col] if spark_col in row.asDict() else None
                if val is not None:
                    data[f'{CF_POLY_BOOKS}:{hbase_col}'.encode()] = str(val).encode()
            
            # Store full market_id
            data[f'{CF_POLY_BOOKS}:market_id'.encode()] = market_id.encode()
            data[f'{CF_POLY_BOOKS}:window_end'.encode()] = str(window_end_ms).encode()
            
            if data:
                table.put(rowkey.encode(), data)
                rows_written += 1
        
        print(f"[HBASE] Poly books batch {batch_id}: wrote {rows_written} rows")
        
    except Exception as e:
        print(f"[ERROR] Poly books HBase write failed: {e}")
    finally:
        conn.close()


def write_poly_trades_batch(batch_df, batch_id):
    """
    Write Polymarket trades aggregation batch to HBase.
    
    Expected columns:
        window, market_id, total_shares_traded, num_trades
    """
    # Use count() for Spark 2.x/3.x compatibility (isEmpty() is Spark 3.3+)
    if batch_df.count() == 0:
        return
    
    conn = _get_connection()
    if conn is None:
        print(f"[DEBUG] Poly trades batch {batch_id}: HBase disabled, skipping write")
        return
    
    try:
        table = conn.table(TABLE_MARKET_LIVE)
        rows_written = 0
        
        for row in batch_df.collect():
            window_end_ms = _extract_window_end_ms(row['window'])
            market_id = row['market_id']
            
            market_id_short = market_id[:16] if len(market_id) > 16 else market_id
            rowkey = generate_speed_rowkey(SPEED_ROW_POLY_TRADES, market_id_short, window_end_ms)
            
            data = {}
            
            column_map = {
                'total_shares_traded': 'total_shares',
                'num_trades': 'num_trades',
            }
            
            for spark_col, hbase_col in column_map.items():
                val = row[spark_col] if spark_col in row.asDict() else None
                if val is not None:
                    data[f'{CF_POLY_TRADES}:{hbase_col}'.encode()] = str(val).encode()
            
            data[f'{CF_POLY_TRADES}:market_id'.encode()] = market_id.encode()
            data[f'{CF_POLY_TRADES}:window_end'.encode()] = str(window_end_ms).encode()
            
            if data:
                table.put(rowkey.encode(), data)
                rows_written += 1
        
        print(f"[HBASE] Poly trades batch {batch_id}: wrote {rows_written} rows")
        
    except Exception as e:
        print(f"[ERROR] Poly trades HBase write failed: {e}")
    finally:
        conn.close()


def set_hbase_enabled(enabled: bool):
    """Enable or disable HBase writes globally."""
    global HBASE_ENABLED
    HBASE_ENABLED = enabled
    print(f"[CONFIG] HBase writes {'enabled' if enabled else 'disabled'}")


def test_connection() -> bool:
    """Test HBase connectivity. Returns True if successful."""
    conn = _get_connection()
    if conn is None:
        return False
    try:
        tables = conn.tables()
        print(f"[TEST] HBase connected. Tables: {[t.decode() for t in tables]}")
        return True
    except Exception as e:
        print(f"[TEST] HBase test failed: {e}")
        return False
    finally:
        conn.close()


if __name__ == '__main__':
    print("HBase Sink Module for Speed Layer")
    print("=" * 50)
    print("\nTesting HBase connection...")
    if test_connection():
        print("✓ Connection successful!")
    else:
        print("✗ Connection failed. Ensure HBase Thrift server is running.")
