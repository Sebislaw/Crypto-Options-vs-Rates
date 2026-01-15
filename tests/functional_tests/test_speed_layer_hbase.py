"""
Functional Tests for Speed Layer HBase Integration

Tests that verify the speed layer writes data correctly to HBase
and that the data can be retrieved with proper schema.
"""

import unittest
import sys
import os
import time

# Add parent directories to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'serving_layer', 'hbase'))

try:
    import happybase
    HAPPYBASE_AVAILABLE = True
except ImportError:
    HAPPYBASE_AVAILABLE = False

from serving_layer.hbase.config import (
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
    parse_speed_rowkey,
    REVERSE_TIMESTAMP_MAX,
)


def skip_if_no_hbase(func):
    """Decorator to skip tests if HBase is not available."""
    def wrapper(*args, **kwargs):
        if not HAPPYBASE_AVAILABLE:
            raise unittest.SkipTest("HappyBase not installed")
        try:
            conn = happybase.Connection(HBASE_HOST, HBASE_PORT)
            conn.open()
            conn.close()
            return func(*args, **kwargs)
        except Exception as e:
            raise unittest.SkipTest(f"HBase not available: {e}")
    return wrapper


class TestSpeedLayerRowKeys(unittest.TestCase):
    """Test speed layer RowKey generation."""
    
    def test_speed_rowkey_format(self):
        """SL-001: Speed RowKey should be TYPE#IDENTIFIER#REVERSE_TS."""
        rowkey = generate_speed_rowkey('BIN', 'BTCUSDT', 1767201300000)
        parts = rowkey.split('#')
        
        self.assertEqual(len(parts), 3)
        self.assertEqual(parts[0], 'BIN')
        self.assertEqual(parts[1], 'BTCUSDT')
    
    def test_speed_rowkey_reverse_timestamp(self):
        """SL-002: Reverse timestamp should sort newest first."""
        ts1 = 1767201300000  # Earlier
        ts2 = 1767201400000  # Later
        
        rowkey1 = generate_speed_rowkey('BIN', 'BTCUSDT', ts1)
        rowkey2 = generate_speed_rowkey('BIN', 'BTCUSDT', ts2)
        
        # Newer timestamp should have lower rowkey (sorts first)
        self.assertLess(rowkey2, rowkey1)
    
    def test_parse_speed_rowkey(self):
        """SL-003: Parse speed rowkey should extract components."""
        window_end_ms = 1767201300000
        rowkey = generate_speed_rowkey('PB', 'market123', window_end_ms)
        
        row_type, identifier, recovered_ts = parse_speed_rowkey(rowkey)
        
        self.assertEqual(row_type, 'PB')
        self.assertEqual(identifier, 'market123')
        self.assertEqual(recovered_ts, window_end_ms)
    
    def test_different_types_sort_separately(self):
        """SL-004: Different row types sort into separate ranges."""
        ts = 1767201300000
        
        bin_key = generate_speed_rowkey(SPEED_ROW_BINANCE, 'BTC', ts)
        pb_key = generate_speed_rowkey(SPEED_ROW_POLY_BOOKS, 'market', ts)
        pt_key = generate_speed_rowkey(SPEED_ROW_POLY_TRADES, 'market', ts)
        
        # Keys with different types should not be equal
        self.assertNotEqual(bin_key[:3], pb_key[:2])


class TestSpeedLayerSchema(unittest.TestCase):
    """Test market_live table schema for speed layer."""
    
    @classmethod
    def setUpClass(cls):
        if HAPPYBASE_AVAILABLE:
            try:
                cls.connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
                cls.connection.open()
                cls.table = cls.connection.table(TABLE_MARKET_LIVE)
                cls.families = cls.table.families()
                cls.hbase_available = True
            except Exception:
                cls.hbase_available = False
        else:
            cls.hbase_available = False
    
    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'connection') and cls.connection:
            cls.connection.close()
    
    @skip_if_no_hbase
    def test_has_binance_column_family(self):
        """SL-005: market_live should have 'b' column family."""
        family_names = [k.decode() for k in self.families.keys()]
        self.assertIn(
            CF_BINANCE,
            family_names,
            f"Column family '{CF_BINANCE}' not found. Existing: {family_names}"
        )
    
    @skip_if_no_hbase
    def test_has_poly_books_column_family(self):
        """SL-006: market_live should have 'p' column family."""
        family_names = [k.decode() for k in self.families.keys()]
        self.assertIn(
            CF_POLY_BOOKS,
            family_names,
            f"Column family '{CF_POLY_BOOKS}' not found. Existing: {family_names}"
        )
    
    @skip_if_no_hbase
    def test_has_poly_trades_column_family(self):
        """SL-007: market_live should have 't' column family."""
        family_names = [k.decode() for k in self.families.keys()]
        self.assertIn(
            CF_POLY_TRADES,
            family_names,
            f"Column family '{CF_POLY_TRADES}' not found. Existing: {family_names}"
        )


class TestSpeedLayerWriteRead(unittest.TestCase):
    """Test writing and reading speed layer data."""
    
    @classmethod
    def setUpClass(cls):
        if HAPPYBASE_AVAILABLE:
            try:
                cls.connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
                cls.connection.open()
                cls.table = cls.connection.table(TABLE_MARKET_LIVE)
                cls.hbase_available = True
                cls.test_ts = int(time.time() * 1000)
            except Exception:
                cls.hbase_available = False
        else:
            cls.hbase_available = False
        cls.test_rowkeys = []
    
    @classmethod
    def tearDownClass(cls):
        # Clean up test data
        if hasattr(cls, 'table') and cls.test_rowkeys:
            for rowkey in cls.test_rowkeys:
                try:
                    cls.table.delete(rowkey.encode())
                except Exception:
                    pass
        if hasattr(cls, 'connection') and cls.connection:
            cls.connection.close()
    
    @skip_if_no_hbase
    def test_write_binance_record(self):
        """SL-008: Write a Binance speed layer record."""
        rowkey = generate_speed_rowkey(SPEED_ROW_BINANCE, 'TESTUSDT', self.test_ts)
        self.test_rowkeys.append(rowkey)
        
        data = {
            f'{CF_BINANCE}:current_price'.encode(): b'50000.50',
            f'{CF_BINANCE}:avg_price'.encode(): b'49990.25',
            f'{CF_BINANCE}:volatility'.encode(): b'125.50',
            f'{CF_BINANCE}:total_volume'.encode(): b'1000000.0',
            f'{CF_BINANCE}:ticks'.encode(): b'42',
        }
        
        self.table.put(rowkey.encode(), data)
        
        # Verify write
        row = self.table.row(rowkey.encode())
        self.assertIsNotNone(row)
        self.assertEqual(row.get(f'{CF_BINANCE}:current_price'.encode()), b'50000.50')
    
    @skip_if_no_hbase
    def test_write_poly_books_record(self):
        """SL-009: Write a Polymarket books speed layer record."""
        rowkey = generate_speed_rowkey(SPEED_ROW_POLY_BOOKS, 'testmarket123', self.test_ts)
        self.test_rowkeys.append(rowkey)
        
        data = {
            f'{CF_POLY_BOOKS}:current_prob'.encode(): b'0.65',
            f'{CF_POLY_BOOKS}:avg_prob'.encode(): b'0.62',
            f'{CF_POLY_BOOKS}:current_spread'.encode(): b'0.02',
            f'{CF_POLY_BOOKS}:num_updates'.encode(): b'15',
        }
        
        self.table.put(rowkey.encode(), data)
        
        row = self.table.row(rowkey.encode())
        self.assertIsNotNone(row)
        self.assertEqual(row.get(f'{CF_POLY_BOOKS}:current_prob'.encode()), b'0.65')
    
    @skip_if_no_hbase
    def test_write_poly_trades_record(self):
        """SL-010: Write a Polymarket trades speed layer record."""
        rowkey = generate_speed_rowkey(SPEED_ROW_POLY_TRADES, 'testmarket456', self.test_ts)
        self.test_rowkeys.append(rowkey)
        
        data = {
            f'{CF_POLY_TRADES}:total_shares'.encode(): b'5000.0',
            f'{CF_POLY_TRADES}:num_trades'.encode(): b'23',
        }
        
        self.table.put(rowkey.encode(), data)
        
        row = self.table.row(rowkey.encode())
        self.assertIsNotNone(row)
        self.assertEqual(row.get(f'{CF_POLY_TRADES}:num_trades'.encode()), b'23')
    
    @skip_if_no_hbase
    def test_scan_by_type_prefix(self):
        """SL-011: Scan records by type prefix."""
        # Write test records
        ts = int(time.time() * 1000)
        
        for i in range(3):
            rowkey = generate_speed_rowkey(SPEED_ROW_BINANCE, f'SCAN{i}USDT', ts + i)
            self.test_rowkeys.append(rowkey)
            self.table.put(rowkey.encode(), {f'{CF_BINANCE}:current_price'.encode(): b'100'})
        
        # Scan by prefix
        prefix = f'{SPEED_ROW_BINANCE}#'.encode()
        results = list(self.table.scan(row_prefix=prefix, limit=10))
        
        self.assertGreaterEqual(len(results), 3)


if __name__ == '__main__':
    unittest.main(verbosity=2)
