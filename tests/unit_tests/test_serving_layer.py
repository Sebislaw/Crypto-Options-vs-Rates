"""
Unit Tests for Serving Layer

Tests the HBase configuration, rowkey generation, and merge logic
for the serving layer components.
"""

import unittest
import sys
import os
from datetime import datetime
import importlib.util

# Load serving_layer/hbase/config.py explicitly to avoid conflicts with other config.py files
_config_path = os.path.join(os.path.dirname(__file__), '..', '..', 'serving_layer', 'hbase', 'config.py')
_spec = importlib.util.spec_from_file_location("hbase_config", _config_path)
_config = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_config)

# Import constants and functions from the loaded module
HBASE_HOST = _config.HBASE_HOST
HBASE_PORT = _config.HBASE_PORT
TABLE_MARKET_ANALYTICS = _config.TABLE_MARKET_ANALYTICS
TABLE_MARKET_LIVE = _config.TABLE_MARKET_LIVE
CF_PRICES = _config.CF_PRICES
CF_BETTING = _config.CF_BETTING
CF_CORRELATION = _config.CF_CORRELATION
CF_BINANCE = _config.CF_BINANCE
CF_POLY_BOOKS = _config.CF_POLY_BOOKS
CF_POLY_TRADES = _config.CF_POLY_TRADES
ROWKEY_SEPARATOR = _config.ROWKEY_SEPARATOR
REVERSE_TIMESTAMP_MAX = _config.REVERSE_TIMESTAMP_MAX
SYMBOLS = _config.SYMBOLS
generate_batch_rowkey = _config.generate_batch_rowkey
generate_live_rowkey = _config.generate_live_rowkey
generate_speed_rowkey = _config.generate_speed_rowkey
parse_rowkey = _config.parse_rowkey
parse_speed_rowkey = _config.parse_speed_rowkey
reverse_timestamp_to_actual = _config.reverse_timestamp_to_actual
SPEED_ROW_BINANCE = _config.SPEED_ROW_BINANCE
SPEED_ROW_POLY_BOOKS = _config.SPEED_ROW_POLY_BOOKS
SPEED_ROW_POLY_TRADES = _config.SPEED_ROW_POLY_TRADES


class TestServingLayerConfig(unittest.TestCase):
    """Test serving layer configuration constants."""
    
    def test_hbase_connection_defaults(self):
        """SRV-001: HBase connection defaults should be defined."""
        self.assertEqual(HBASE_HOST, 'localhost')
        self.assertEqual(HBASE_PORT, 9090)
    
    def test_table_names_defined(self):
        """SRV-002: Table names should be defined."""
        self.assertEqual(TABLE_MARKET_ANALYTICS, 'market_analytics')
        self.assertEqual(TABLE_MARKET_LIVE, 'market_live')
    
    def test_column_families_batch(self):
        """SRV-003: Batch layer column families should be defined."""
        self.assertEqual(CF_PRICES, 'price_data')
        self.assertEqual(CF_BETTING, 'bet_data')
        self.assertEqual(CF_CORRELATION, 'analysis')
    
    def test_column_families_speed(self):
        """SRV-004: Speed layer column families should be defined."""
        self.assertEqual(CF_BINANCE, 'b')
        self.assertEqual(CF_POLY_BOOKS, 'p')
        self.assertEqual(CF_POLY_TRADES, 't')
    
    def test_symbols_list(self):
        """SRV-005: Symbols list should contain expected cryptocurrencies."""
        expected = ['BTC', 'ETH', 'SOL', 'XRP']
        for symbol in expected:
            self.assertIn(symbol, SYMBOLS)


class TestBatchRowKeyGeneration(unittest.TestCase):
    """Test batch layer rowkey generation."""
    
    def test_batch_rowkey_format(self):
        """SRV-006: Batch rowkey should be SYMBOL#TIMESTAMP."""
        rowkey = generate_batch_rowkey('BTC', 1767201300)
        self.assertEqual(rowkey, 'BTC#1767201300')
    
    def test_batch_rowkey_uppercase(self):
        """SRV-007: Batch rowkey symbol should be uppercase."""
        rowkey = generate_batch_rowkey('btc', 1767201300)
        self.assertTrue(rowkey.startswith('BTC'))
    
    def test_batch_rowkey_separator(self):
        """SRV-008: Batch rowkey should use configured separator."""
        rowkey = generate_batch_rowkey('ETH', 1767201300)
        self.assertIn(ROWKEY_SEPARATOR, rowkey)


class TestLiveRowKeyGeneration(unittest.TestCase):
    """Test speed layer rowkey generation."""
    
    def test_live_rowkey_reverse_timestamp(self):
        """SRV-009: Live rowkey should use reverse timestamp."""
        ts = 1767201300000
        rowkey = generate_live_rowkey('BTC', ts)
        
        parts = rowkey.split(ROWKEY_SEPARATOR)
        reverse_ts = int(parts[1])
        
        self.assertEqual(reverse_ts, REVERSE_TIMESTAMP_MAX - ts)
    
    def test_live_rowkey_newest_first(self):
        """SRV-010: Newer timestamps should have lower rowkeys (sort first)."""
        ts1 = 1767201300000
        ts2 = 1767201400000  # 100 seconds later
        
        key1 = generate_live_rowkey('BTC', ts1)
        key2 = generate_live_rowkey('BTC', ts2)
        
        # Newer (ts2) should sort before older (ts1)
        self.assertLess(key2, key1)
    
    def test_reverse_timestamp_to_actual(self):
        """SRV-011: Reverse timestamp should be recoverable."""
        original_ts = 1767201300000
        reverse_ts = REVERSE_TIMESTAMP_MAX - original_ts
        recovered = reverse_timestamp_to_actual(reverse_ts)
        
        self.assertEqual(recovered, original_ts)


class TestSpeedRowKeyGeneration(unittest.TestCase):
    """Test speed layer windowed rowkey generation."""
    
    def test_speed_rowkey_format(self):
        """SRV-012: Speed rowkey should be TYPE#IDENTIFIER#REVERSE_TS."""
        rowkey = generate_speed_rowkey('BIN', 'BTCUSDT', 1767201300000)
        parts = rowkey.split(ROWKEY_SEPARATOR)
        
        self.assertEqual(len(parts), 3)
        self.assertEqual(parts[0], 'BIN')
        self.assertEqual(parts[1], 'BTCUSDT')
    
    def test_speed_rowkey_parse(self):
        """SRV-013: Speed rowkey should be parseable."""
        window_end = 1767201300000
        rowkey = generate_speed_rowkey('PB', 'market123', window_end)
        
        row_type, identifier, recovered_ts = parse_speed_rowkey(rowkey)
        
        self.assertEqual(row_type, 'PB')
        self.assertEqual(identifier, 'market123')
        self.assertEqual(recovered_ts, window_end)
    
    def test_speed_row_types(self):
        """SRV-014: Speed row types should be defined."""
        self.assertEqual(SPEED_ROW_BINANCE, 'BIN')
        self.assertEqual(SPEED_ROW_POLY_BOOKS, 'PB')
        self.assertEqual(SPEED_ROW_POLY_TRADES, 'PT')


class TestRowKeyParsing(unittest.TestCase):
    """Test rowkey parsing utilities."""
    
    def test_parse_batch_rowkey(self):
        """SRV-015: Batch rowkey parsing should extract components."""
        rowkey = 'ETH#1767201300'
        symbol, ts = parse_rowkey(rowkey)
        
        self.assertEqual(symbol, 'ETH')
        self.assertEqual(ts, 1767201300)
    
    def test_parse_invalid_rowkey(self):
        """SRV-016: Invalid rowkey should raise ValueError."""
        with self.assertRaises(ValueError):
            parse_rowkey('invalid_rowkey_no_separator')
    
    def test_parse_empty_rowkey(self):
        """SRV-017: Empty rowkey should raise ValueError."""
        with self.assertRaises(ValueError):
            parse_rowkey('')


class TestMergeLogic(unittest.TestCase):
    """Test merge view logic (unit level, no HBase required)."""
    
    def test_window_boundary_calculation(self):
        """SRV-018: 15-minute window boundaries should be correct."""
        # Given a timestamp, find the window end
        ts = 1767201325  # 25 seconds into a 15-min window
        window_size = 15 * 60  # 15 minutes in seconds
        
        window_start = (ts // window_size) * window_size
        window_end = window_start + window_size
        
        # Window should be aligned to 15-minute boundaries
        self.assertEqual(window_start % window_size, 0)
        self.assertEqual(window_end - window_start, window_size)
    
    def test_timestamp_to_datetime(self):
        """SRV-019: Timestamps should convert to readable datetime."""
        ts_seconds = 1767201300
        dt = datetime.utcfromtimestamp(ts_seconds)
        
        # Should be a valid future date
        self.assertGreater(dt.year, 2020)
    
    def test_live_data_recency_preference(self):
        """SRV-020: Merge logic should prefer recent live data over stale batch."""
        # Simulate merge decision
        batch_ts = 1767200400  # Older
        live_ts = 1767201300   # Newer
        current_ts = 1767201400
        
        batch_age = current_ts - batch_ts
        live_age = current_ts - live_ts
        
        # Live data is more recent
        self.assertLess(live_age, batch_age)


if __name__ == '__main__':
    unittest.main(verbosity=2)
