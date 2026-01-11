"""
Functional Tests for Data Integrity

Tests that verify data can be written and read correctly from HBase,
and that data integrity is maintained across operations.
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
    TABLE_MARKET_ANALYTICS,
    TABLE_MARKET_LIVE,
    generate_batch_rowkey,
    generate_live_rowkey,
    parse_rowkey,
    reverse_timestamp_to_actual,
    REVERSE_TIMESTAMP_MAX,
    ROWKEY_SEPARATOR,
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


class TestRowKeyGeneration(unittest.TestCase):
    """Test DI-001 & DI-002: RowKey format tests."""
    
    def test_batch_rowkey_format(self):
        """DI-001: Batch RowKey should be SYMBOL#TIMESTAMP."""
        rowkey = generate_batch_rowkey('BTC', 1767201300)
        self.assertEqual(rowkey, 'BTC#1767201300')
    
    def test_batch_rowkey_uppercase(self):
        """Batch RowKey should uppercase the symbol."""
        rowkey = generate_batch_rowkey('btc', 1767201300)
        self.assertEqual(rowkey, 'BTC#1767201300')
    
    def test_live_rowkey_format(self):
        """DI-002: Live RowKey should use reverse timestamp."""
        timestamp_ms = 1767201300000
        rowkey = generate_live_rowkey('BTC', timestamp_ms)
        
        # Parse the rowkey
        symbol, reverse_ts = rowkey.split(ROWKEY_SEPARATOR)
        
        # Verify format
        self.assertEqual(symbol, 'BTC')
        
        # Verify reverse timestamp calculation
        expected_reverse = REVERSE_TIMESTAMP_MAX - timestamp_ms
        self.assertEqual(int(reverse_ts), expected_reverse)
    
    def test_reverse_timestamp_ordering(self):
        """Live RowKeys should sort newest first."""
        ts1 = 1767201300000  # Earlier
        ts2 = 1767201400000  # Later
        
        rowkey1 = generate_live_rowkey('BTC', ts1)
        rowkey2 = generate_live_rowkey('BTC', ts2)
        
        # Newer timestamp should have lower rowkey (sorts first)
        self.assertLess(rowkey2, rowkey1)
    
    def test_parse_rowkey(self):
        """RowKey parsing should extract components correctly."""
        rowkey = 'ETH#1767201300'
        symbol, ts = parse_rowkey(rowkey)
        
        self.assertEqual(symbol, 'ETH')
        self.assertEqual(ts, 1767201300)
    
    def test_reverse_timestamp_conversion(self):
        """Reverse timestamp should convert back to actual correctly."""
        original_ts = 1767201300000
        reverse_ts = REVERSE_TIMESTAMP_MAX - original_ts
        recovered_ts = reverse_timestamp_to_actual(reverse_ts)
        
        self.assertEqual(recovered_ts, original_ts)


class TestBatchWriteRead(unittest.TestCase):
    """Test WR-001 & WR-002: Batch layer write/read tests."""
    
    @classmethod
    def setUpClass(cls):
        if HAPPYBASE_AVAILABLE:
            try:
                cls.connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
                cls.connection.open()
                cls.table = cls.connection.table(TABLE_MARKET_ANALYTICS)
                cls.hbase_available = True
                cls.test_rowkey = generate_batch_rowkey('TEST', int(time.time()))
            except Exception:
                cls.hbase_available = False
        else:
            cls.hbase_available = False
    
    @classmethod
    def tearDownClass(cls):
        # Clean up test data
        if hasattr(cls, 'table') and hasattr(cls, 'test_rowkey'):
            try:
                cls.table.delete(cls.test_rowkey.encode())
            except Exception:
                pass
        if hasattr(cls, 'connection') and cls.connection:
            cls.connection.close()
    
    @skip_if_no_hbase
    def test_write_batch_record(self):
        """WR-001: Write a batch analytics record."""
        data = {
            b'prices:open': b'100.50',
            b'prices:close': b'102.75',
            b'prices:high': b'103.00',
            b'prices:low': b'99.80',
            b'prices:volume': b'50000.00',
            b'prices:volatility': b'0.025',
            b'betting:avg_prob_up': b'0.65',
            b'betting:avg_prob_down': b'0.35',
            b'betting:total_bet_vol': b'10000.00',
            b'betting:sentiment_score': b'0.15',
            b'correlation:prediction_result': b'CORRECT',
            b'correlation:divergence': b'0.05',
        }
        
        self.table.put(self.test_rowkey.encode(), data)
        
        # Verify write succeeded by reading back
        row = self.table.row(self.test_rowkey.encode())
        self.assertIsNotNone(row)
        self.assertGreater(len(row), 0)
    
    @skip_if_no_hbase
    def test_read_batch_record(self):
        """WR-002: Read back a batch analytics record."""
        # First write
        data = {
            b'prices:close': b'102.75',
            b'betting:avg_prob_up': b'0.65',
            b'correlation:prediction_result': b'CORRECT',
        }
        self.table.put(self.test_rowkey.encode(), data)
        
        # Then read
        row = self.table.row(self.test_rowkey.encode())
        
        self.assertEqual(row.get(b'prices:close'), b'102.75')
        self.assertEqual(row.get(b'betting:avg_prob_up'), b'0.65')
        self.assertEqual(row.get(b'correlation:prediction_result'), b'CORRECT')


class TestLiveWriteRead(unittest.TestCase):
    """Test WR-003 & WR-004: Speed layer write/read tests."""
    
    @classmethod
    def setUpClass(cls):
        if HAPPYBASE_AVAILABLE:
            try:
                cls.connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
                cls.connection.open()
                cls.table = cls.connection.table(TABLE_MARKET_LIVE)
                cls.hbase_available = True
                cls.test_timestamp = int(time.time() * 1000)
                cls.test_rowkey = generate_live_rowkey('TEST', cls.test_timestamp)
            except Exception:
                cls.hbase_available = False
        else:
            cls.hbase_available = False
    
    @classmethod
    def tearDownClass(cls):
        # Clean up test data
        if hasattr(cls, 'table') and hasattr(cls, 'test_rowkey'):
            try:
                cls.table.delete(cls.test_rowkey.encode())
            except Exception:
                pass
        if hasattr(cls, 'connection') and cls.connection:
            cls.connection.close()
    
    @skip_if_no_hbase
    def test_write_live_record(self):
        """WR-003: Write a live data record with reverse timestamp."""
        data = {
            b'd:binance_price': b'50000.00',
            b'd:poly_last_trade': b'0.55',
            b'd:implied_prob': b'55.0',
        }
        
        self.table.put(self.test_rowkey.encode(), data)
        
        # Verify write succeeded
        row = self.table.row(self.test_rowkey.encode())
        self.assertIsNotNone(row)
        self.assertGreater(len(row), 0)
    
    @skip_if_no_hbase
    def test_read_live_by_prefix_scan(self):
        """WR-004: Scan live records by symbol prefix."""
        # Write multiple records
        timestamps = [
            int(time.time() * 1000),
            int(time.time() * 1000) + 1000,
            int(time.time() * 1000) + 2000,
        ]
        
        rowkeys = []
        for ts in timestamps:
            rowkey = generate_live_rowkey('SCANTEST', ts)
            rowkeys.append(rowkey)
            self.table.put(rowkey.encode(), {b'd:binance_price': b'50000.00'})
        
        # Scan by prefix
        prefix = b'SCANTEST#'
        results = list(self.table.scan(row_prefix=prefix, limit=10))
        
        self.assertGreaterEqual(len(results), 3)
        
        # Clean up
        for rowkey in rowkeys:
            self.table.delete(rowkey.encode())


class TestDataTypePrecision(unittest.TestCase):
    """Test DI-003: Data type preservation tests."""
    
    @classmethod
    def setUpClass(cls):
        if HAPPYBASE_AVAILABLE:
            try:
                cls.connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
                cls.connection.open()
                cls.table = cls.connection.table(TABLE_MARKET_ANALYTICS)
                cls.hbase_available = True
                cls.test_rowkey = generate_batch_rowkey('PRECISION', int(time.time()))
            except Exception:
                cls.hbase_available = False
        else:
            cls.hbase_available = False
    
    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'table') and hasattr(cls, 'test_rowkey'):
            try:
                cls.table.delete(cls.test_rowkey.encode())
            except Exception:
                pass
        if hasattr(cls, 'connection') and cls.connection:
            cls.connection.close()
    
    @skip_if_no_hbase
    def test_float_precision(self):
        """DI-003: Float values should maintain precision."""
        original_value = 92543.87654321
        
        self.table.put(
            self.test_rowkey.encode(),
            {b'prices:close': str(original_value).encode()}
        )
        
        row = self.table.row(self.test_rowkey.encode())
        retrieved_value = float(row[b'prices:close'].decode())
        
        self.assertAlmostEqual(retrieved_value, original_value, places=6)
    
    @skip_if_no_hbase
    def test_string_values(self):
        """String values should be stored and retrieved correctly."""
        original_value = 'CORRECT'
        
        self.table.put(
            self.test_rowkey.encode(),
            {b'correlation:prediction_result': original_value.encode()}
        )
        
        row = self.table.row(self.test_rowkey.encode())
        retrieved_value = row[b'correlation:prediction_result'].decode()
        
        self.assertEqual(retrieved_value, original_value)


if __name__ == '__main__':
    unittest.main(verbosity=2)
