"""
HBase Writer Module for Crypto Options vs Rates Project

Provides standalone HBase write functionality using HappyBase.
Used for testing and simple Python scripts outside of Spark context.
"""

import sys
import time
from typing import Dict, List, Optional, Any

try:
    import happybase
except ImportError:
    print("HappyBase not installed. Install with: pip install happybase")
    happybase = None

from config import (
    HBASE_HOST,
    HBASE_PORT,
    TABLE_MARKET_ANALYTICS,
    TABLE_MARKET_LIVE,
    CF_PRICES,
    CF_BETTING,
    CF_CORRELATION,
    CF_DATA,
    generate_batch_rowkey,
    generate_live_rowkey,
    REVERSE_TIMESTAMP_MAX,
)


class HBaseWriter:
    """
    HBase writer class for the serving layer.
    
    Provides methods to write data to market_analytics (batch) and
    market_live (speed) tables.
    """
    
    def __init__(self, host: str = HBASE_HOST, port: int = HBASE_PORT):
        """
        Initialize HBase connection.
        
        Args:
            host: HBase Thrift server host
            port: HBase Thrift server port
        """
        self.host = host
        self.port = port
        self.connection = None
        self._connect()
    
    def _connect(self):
        """Establish connection to HBase."""
        if happybase is None:
            raise ImportError("HappyBase is required but not installed")
        
        try:
            self.connection = happybase.Connection(self.host, self.port)
            self.connection.open()
        except Exception as e:
            print(f"Failed to connect to HBase at {self.host}:{self.port}: {e}")
            raise
    
    def close(self):
        """Close the HBase connection."""
        if self.connection:
            self.connection.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    # --------------------------------------------------------------------------
    # Batch Layer Writers (market_analytics)
    # --------------------------------------------------------------------------
    
    def write_batch_analytics(
        self,
        symbol: str,
        window_end_ts: int,
        prices_data: Dict[str, Any],
        betting_data: Dict[str, Any],
        correlation_data: Dict[str, Any]
    ) -> str:
        """
        Write a batch analytics record to market_analytics table.
        
        Args:
            symbol: Cryptocurrency symbol (e.g., 'BTC')
            window_end_ts: End timestamp of the 15-min window
            prices_data: Dict with keys: open, close, high, low, volume, volatility
            betting_data: Dict with keys: avg_prob_up, avg_prob_down, total_bet_vol, sentiment_score
            correlation_data: Dict with keys: prediction_result, divergence
            
        Returns:
            The generated RowKey
        """
        rowkey = generate_batch_rowkey(symbol, window_end_ts)
        table = self.connection.table(TABLE_MARKET_ANALYTICS)
        
        # Build the data dict with column family prefixes
        data = {}
        
        # prices column family
        for key, value in prices_data.items():
            data[f'{CF_PRICES}:{key}'.encode()] = str(value).encode()
        
        # betting column family
        for key, value in betting_data.items():
            data[f'{CF_BETTING}:{key}'.encode()] = str(value).encode()
        
        # correlation column family
        for key, value in correlation_data.items():
            data[f'{CF_CORRELATION}:{key}'.encode()] = str(value).encode()
        
        table.put(rowkey.encode(), data)
        return rowkey
    
    def batch_write_analytics(self, records: List[Dict]) -> List[str]:
        """
        Write multiple batch analytics records efficiently.
        
        Args:
            records: List of dicts with keys: symbol, window_end_ts, prices, betting, correlation
            
        Returns:
            List of generated RowKeys
        """
        table = self.connection.table(TABLE_MARKET_ANALYTICS)
        rowkeys = []
        
        with table.batch() as batch:
            for record in records:
                rowkey = generate_batch_rowkey(record['symbol'], record['window_end_ts'])
                rowkeys.append(rowkey)
                
                data = {}
                for key, value in record.get('prices', {}).items():
                    data[f'{CF_PRICES}:{key}'.encode()] = str(value).encode()
                for key, value in record.get('betting', {}).items():
                    data[f'{CF_BETTING}:{key}'.encode()] = str(value).encode()
                for key, value in record.get('correlation', {}).items():
                    data[f'{CF_CORRELATION}:{key}'.encode()] = str(value).encode()
                
                batch.put(rowkey.encode(), data)
        
        return rowkeys
    
    # --------------------------------------------------------------------------
    # Speed Layer Writers (market_live)
    # --------------------------------------------------------------------------
    
    def write_live_data(
        self,
        symbol: str,
        timestamp: int,
        binance_price: float,
        poly_last_trade: Optional[float] = None,
        poly_best_bid: Optional[float] = None,
        poly_best_ask: Optional[float] = None,
        implied_prob: Optional[float] = None
    ) -> str:
        """
        Write a live data record to market_live table.
        
        Args:
            symbol: Cryptocurrency symbol
            timestamp: Event timestamp (milliseconds)
            binance_price: Current Binance price
            poly_last_trade: Last Polymarket trade price (0-1)
            poly_best_bid: Best bid price
            poly_best_ask: Best ask price
            implied_prob: Implied probability percentage
            
        Returns:
            The generated RowKey
        """
        rowkey = generate_live_rowkey(symbol, timestamp)
        table = self.connection.table(TABLE_MARKET_LIVE)
        
        data = {
            f'{CF_DATA}:binance_price'.encode(): str(binance_price).encode(),
        }
        
        if poly_last_trade is not None:
            data[f'{CF_DATA}:poly_last_trade'.encode()] = str(poly_last_trade).encode()
        if poly_best_bid is not None:
            data[f'{CF_DATA}:poly_best_bid'.encode()] = str(poly_best_bid).encode()
        if poly_best_ask is not None:
            data[f'{CF_DATA}:poly_best_ask'.encode()] = str(poly_best_ask).encode()
        if implied_prob is not None:
            data[f'{CF_DATA}:implied_prob'.encode()] = str(implied_prob).encode()
        
        table.put(rowkey.encode(), data)
        return rowkey
    
    # --------------------------------------------------------------------------
    # Reader Methods (for testing/validation)
    # --------------------------------------------------------------------------
    
    def get_batch_record(self, symbol: str, window_end_ts: int) -> Optional[Dict]:
        """
        Retrieve a batch analytics record.
        
        Args:
            symbol: Cryptocurrency symbol
            window_end_ts: End timestamp of the window
            
        Returns:
            Dict with record data or None if not found
        """
        rowkey = generate_batch_rowkey(symbol, window_end_ts)
        table = self.connection.table(TABLE_MARKET_ANALYTICS)
        row = table.row(rowkey.encode())
        
        if not row:
            return None
        
        # Parse the row data
        result = {'rowkey': rowkey, 'prices': {}, 'betting': {}, 'correlation': {}}
        for key, value in row.items():
            cf, col = key.decode().split(':')
            result[cf][col] = value.decode()
        
        return result
    
    def get_live_records(self, symbol: str, limit: int = 10) -> List[Dict]:
        """
        Retrieve recent live data records for a symbol.
        
        Args:
            symbol: Cryptocurrency symbol
            limit: Maximum number of records to return
            
        Returns:
            List of record dicts (newest first due to reverse timestamp)
        """
        table = self.connection.table(TABLE_MARKET_LIVE)
        
        # Scan with row prefix for the symbol
        prefix = f"{symbol}#".encode()
        records = []
        
        for rowkey, data in table.scan(row_prefix=prefix, limit=limit):
            record = {'rowkey': rowkey.decode(), 'data': {}}
            for key, value in data.items():
                _, col = key.decode().split(':')
                record['data'][col] = value.decode()
            records.append(record)
        
        return records
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        tables = [t.decode() for t in self.connection.tables()]
        return table_name in tables


def run_self_test():
    """Run a self-test to verify HBase connectivity and table operations."""
    print("=" * 60)
    print("Running HBase Writer Self-Test")
    print("=" * 60)
    
    try:
        with HBaseWriter() as writer:
            # Check tables exist
            print("\n[1] Checking tables exist...")
            if writer.table_exists(TABLE_MARKET_ANALYTICS):
                print(f"    ✓ Table '{TABLE_MARKET_ANALYTICS}' exists")
            else:
                print(f"    ✗ Table '{TABLE_MARKET_ANALYTICS}' NOT FOUND")
                return False
            
            if writer.table_exists(TABLE_MARKET_LIVE):
                print(f"    ✓ Table '{TABLE_MARKET_LIVE}' exists")
            else:
                print(f"    ✗ Table '{TABLE_MARKET_LIVE}' NOT FOUND")
                return False
            
            # Test batch write
            print("\n[2] Testing batch layer write...")
            test_ts = int(time.time())
            rowkey = writer.write_batch_analytics(
                symbol='TEST',
                window_end_ts=test_ts,
                prices_data={'open': 100.0, 'close': 101.5, 'high': 102.0, 'low': 99.5, 'volume': 1000.0, 'volatility': 0.025},
                betting_data={'avg_prob_up': 0.65, 'avg_prob_down': 0.35, 'total_bet_vol': 5000.0, 'sentiment_score': 0.15},
                correlation_data={'prediction_result': 'CORRECT', 'divergence': 0.05}
            )
            print(f"    ✓ Wrote batch record with RowKey: {rowkey}")
            
            # Test batch read
            print("\n[3] Testing batch layer read...")
            record = writer.get_batch_record('TEST', test_ts)
            if record:
                print(f"    ✓ Read back record: prices.close={record['prices'].get('close')}")
            else:
                print("    ✗ Failed to read back record")
                return False
            
            # Test live write
            print("\n[4] Testing speed layer write...")
            live_ts = int(time.time() * 1000)  # milliseconds
            live_rowkey = writer.write_live_data(
                symbol='TEST',
                timestamp=live_ts,
                binance_price=50000.00,
                poly_last_trade=0.55,
                implied_prob=55.0
            )
            print(f"    ✓ Wrote live record with RowKey: {live_rowkey}")
            
            # Test live read
            print("\n[5] Testing speed layer read...")
            live_records = writer.get_live_records('TEST', limit=5)
            if live_records:
                print(f"    ✓ Read back {len(live_records)} live record(s)")
            else:
                print("    ✗ Failed to read back live records")
                return False
            
            print("\n" + "=" * 60)
            print("✓ All tests passed! HBase connectivity verified.")
            print("=" * 60)
            return True
            
    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        return False


if __name__ == '__main__':
    if '--test' in sys.argv:
        success = run_self_test()
        sys.exit(0 if success else 1)
    else:
        print("HBase Writer Module")
        print("Usage: python hbase_writer.py --test")
        print("\nTo use in code:")
        print("  from hbase_writer import HBaseWriter")
        print("  with HBaseWriter() as writer:")
        print("      writer.write_batch_analytics(...)")
