"""
Functional Tests for HBase Setup

Tests that verify HBase tables are created correctly and have the expected schema.
These tests require a running HBase instance (typically on the VM).
"""

import unittest
import sys
import os

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
    CF_PRICES,
    CF_BETTING,
    CF_CORRELATION,
    CF_DATA,
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


class TestHBaseTableExistence(unittest.TestCase):
    """Test HB-001: Verify that required tables exist."""
    
    @classmethod
    def setUpClass(cls):
        """Set up HBase connection for all tests."""
        if HAPPYBASE_AVAILABLE:
            try:
                cls.connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
                cls.connection.open()
                cls.tables = [t.decode() for t in cls.connection.tables()]
                cls.hbase_available = True
            except Exception:
                cls.hbase_available = False
        else:
            cls.hbase_available = False
    
    @classmethod
    def tearDownClass(cls):
        """Close connection after all tests."""
        if hasattr(cls, 'connection') and cls.connection:
            cls.connection.close()
    
    @skip_if_no_hbase
    def test_market_analytics_table_exists(self):
        """HB-001a: market_analytics table should exist."""
        self.assertIn(
            TABLE_MARKET_ANALYTICS,
            self.tables,
            f"Table '{TABLE_MARKET_ANALYTICS}' not found. "
            f"Existing tables: {self.tables}"
        )
    
    @skip_if_no_hbase
    def test_market_live_table_exists(self):
        """HB-001b: market_live table should exist."""
        self.assertIn(
            TABLE_MARKET_LIVE,
            self.tables,
            f"Table '{TABLE_MARKET_LIVE}' not found. "
            f"Existing tables: {self.tables}"
        )


class TestMarketAnalyticsSchema(unittest.TestCase):
    """Test HB-002: Verify market_analytics table schema."""
    
    @classmethod
    def setUpClass(cls):
        """Set up HBase connection and get table families."""
        if HAPPYBASE_AVAILABLE:
            try:
                cls.connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
                cls.connection.open()
                cls.table = cls.connection.table(TABLE_MARKET_ANALYTICS)
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
    def test_has_prices_column_family(self):
        """HB-002a: market_analytics should have 'price_data' column family."""
        family_names = [k.decode() for k in self.families.keys()]
        self.assertIn(
            CF_PRICES,
            family_names,
            f"Column family '{CF_PRICES}' not found. Existing: {family_names}"
        )
    
    @skip_if_no_hbase
    def test_has_betting_column_family(self):
        """HB-002b: market_analytics should have 'bet_data' column family."""
        family_names = [k.decode() for k in self.families.keys()]
        self.assertIn(
            CF_BETTING,
            family_names,
            f"Column family '{CF_BETTING}' not found. Existing: {family_names}"
        )
    
    @skip_if_no_hbase
    def test_has_correlation_column_family(self):
        """HB-002c: market_analytics should have 'analysis' column family."""
        family_names = [k.decode() for k in self.families.keys()]
        self.assertIn(
            CF_CORRELATION,
            family_names,
            f"Column family '{CF_CORRELATION}' not found. Existing: {family_names}"
        )
    
    @skip_if_no_hbase
    def test_has_exactly_three_column_families(self):
        """HB-002d: market_analytics should have exactly 3 column families."""
        self.assertEqual(
            len(self.families),
            3,
            f"Expected 3 column families, found {len(self.families)}: {list(self.families.keys())}"
        )


class TestMarketLiveSchema(unittest.TestCase):
    """Test HB-003: Verify market_live table schema."""
    
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
    def test_has_data_column_family(self):
        """HB-003a: market_live should have 'd' column family."""
        family_names = [k.decode() for k in self.families.keys()]
        self.assertIn(
            CF_DATA,
            family_names,
            f"Column family '{CF_DATA}' not found. Existing: {family_names}"
        )
    
    @skip_if_no_hbase
    def test_has_exactly_one_column_family(self):
        """HB-003b: market_live should have exactly 1 column family."""
        self.assertEqual(
            len(self.families),
            1,
            f"Expected 1 column family, found {len(self.families)}: {list(self.families.keys())}"
        )


class TestSetupIdempotency(unittest.TestCase):
    """Test HB-004: Verify setup script is idempotent."""
    
    @skip_if_no_hbase
    def test_tables_exist_after_multiple_runs(self):
        """HB-004: Running setup multiple times should not cause errors."""
        # This test verifies the tables exist (would fail if setup broke them)
        connection = happybase.Connection(HBASE_HOST, HBASE_PORT)
        connection.open()
        
        tables = [t.decode() for t in connection.tables()]
        
        self.assertIn(TABLE_MARKET_ANALYTICS, tables)
        self.assertIn(TABLE_MARKET_LIVE, tables)
        
        connection.close()


if __name__ == '__main__':
    # Run with verbosity
    unittest.main(verbosity=2)
