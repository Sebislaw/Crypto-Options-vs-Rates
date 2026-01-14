"""
Merge Logic Module for Crypto Options vs Rates Project

Combines batch layer (historical) and speed layer (real-time) views
to provide a unified data response for dashboards and applications.
"""

import time
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

try:
    import happybase
except ImportError:
    print("HappyBase not installed. Install with: pip install happybase")
    happybase = None

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'hbase'))

from config import (
    HBASE_HOST,
    HBASE_PORT,
    TABLE_MARKET_ANALYTICS,
    TABLE_MARKET_LIVE,
    CF_PRICES,
    CF_BETTING,
    CF_CORRELATION,
    CF_DATA,
    REVERSE_TIMESTAMP_MAX,
    ROWKEY_SEPARATOR,
)


class MergeEngine:
    """
    Merge Engine for combining batch and speed layer views.
    
    This implements the Lambda Architecture merge pattern where:
    - Batch views provide accurate, comprehensive historical data
    - Speed views provide low-latency recent data
    - Merge logic combines both for a complete picture
    """
    
    def __init__(self, host: str = HBASE_HOST, port: int = HBASE_PORT):
        """
        Initialize the merge engine.
        
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
        
        self.connection = happybase.Connection(self.host, self.port)
        self.connection.open()
    
    def close(self):
        """Close the HBase connection."""
        if self.connection:
            self.connection.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    # --------------------------------------------------------------------------
    # Core Merge Logic
    # --------------------------------------------------------------------------
    
    def get_merged_view(
        self,
        symbol: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
        include_live: bool = True,
        live_window_minutes: int = 60
    ) -> Dict:
        """
        Get a merged view combining batch analytics and live data.
        
        The merge strategy:
        1. Query batch layer for historical 15-min window analytics
        2. Query speed layer for recent real-time data points
        3. Combine with batch data taking precedence for completed windows
        4. Speed layer fills in the most recent incomplete window
        
        Args:
            symbol: Cryptocurrency symbol (e.g., 'BTC')
            start_ts: Start timestamp (seconds). Default: 24 hours ago
            end_ts: End timestamp (seconds). Default: now
            include_live: Whether to include speed layer data
            live_window_minutes: How many minutes of live data to include
            
        Returns:
            Dict with 'analytics' (batch) and 'live' (speed) data
        """
        now = int(time.time())
        
        if end_ts is None:
            end_ts = now
        if start_ts is None:
            start_ts = end_ts - 86400  # Last 24 hours
        
        result = {
            'symbol': symbol,
            'query_time': datetime.now().isoformat(),
            'time_range': {
                'start': datetime.fromtimestamp(start_ts).isoformat(),
                'end': datetime.fromtimestamp(end_ts).isoformat()
            },
            'analytics': [],  # Batch layer data
            'live': [],       # Speed layer data
            'merged_timeline': []  # Combined view
        }
        
        # Query batch layer (historical analytics)
        result['analytics'] = self._query_batch_layer(symbol, start_ts, end_ts)
        
        # Query speed layer (real-time data)
        if include_live:
            live_start_ms = (now - live_window_minutes * 60) * 1000
            result['live'] = self._query_speed_layer(symbol, limit=100)
        
        # Create merged timeline
        result['merged_timeline'] = self._merge_timelines(
            result['analytics'],
            result['live'],
            now
        )
        
        return result
    
    def _query_batch_layer(
        self,
        symbol: str,
        start_ts: int,
        end_ts: int
    ) -> List[Dict]:
        """
        Query the batch layer (market_analytics) for historical data.
        
        The batch layer uses RowKey format: SYMBOL_REVERSE_TIMESTAMP
        where REVERSE_TIMESTAMP = 9999999999999 - actual_ts * 1000
        
        Args:
            symbol: Cryptocurrency symbol (accepts BTC or BTCUSDT format)
            start_ts: Start timestamp (seconds)
            end_ts: End timestamp (seconds)
            
        Returns:
            List of analytics records
        """
        table = self.connection.table(TABLE_MARKET_ANALYTICS)
        records = []
        
        # Normalize symbol to Binance format if needed (BTC -> BTCUSDT)
        binance_symbol = symbol if symbol.endswith('USDT') else f"{symbol}USDT"
        
        # Batch layer uses underscore separator and reverse timestamps
        # Scan with prefix to get all records for the symbol
        prefix = f"{binance_symbol}_".encode()
        
        for rowkey, data in table.scan(row_prefix=prefix):
            try:
                record = self._parse_batch_record(rowkey.decode(), data)
                # Filter by time range (timestamps are in record)
                record_ts = record.get('window_end_ts', 0)
                if start_ts <= record_ts <= end_ts:
                    records.append(record)
            except Exception as e:
                # Skip malformed records
                continue
        
        return records
    
    def _query_speed_layer(self, symbol: str, limit: int = 100) -> List[Dict]:
        """
        Query the speed layer (market_live) for real-time data.
        
        Speed layer uses RowKey format: TYPE#IDENTIFIER#REVERSE_TIMESTAMP
        where TYPE is BIN (Binance), PB (Polymarket Books), PT (Polymarket Trades)
        
        Args:
            symbol: Cryptocurrency symbol (accepts BTC or BTCUSDT format)
            limit: Maximum number of records
            
        Returns:
            List of live data records (newest first)
        """
        table = self.connection.table(TABLE_MARKET_LIVE)
        records = []
        
        # Normalize symbol to Binance format if needed (BTC -> BTCUSDT)
        binance_symbol = symbol if symbol.endswith('USDT') else f"{symbol}USDT"
        
        # Speed layer uses TYPE#IDENTIFIER#REVERSE_TS format
        # First try scanning for Binance data with BIN# prefix
        for row_type in ['BIN']:
            prefix = f"{row_type}{ROWKEY_SEPARATOR}{binance_symbol}{ROWKEY_SEPARATOR}".encode()
            
            for rowkey, data in table.scan(row_prefix=prefix, limit=limit):
                try:
                    record = self._parse_live_record(rowkey.decode(), data)
                    records.append(record)
                except Exception:
                    continue
        
        return records
    
    def _parse_batch_record(self, rowkey: str, data: Dict) -> Dict:
        """
        Parse a batch layer HBase row into a structured dict.
        
        Batch layer RowKey format: SYMBOL_REVERSE_TIMESTAMP
        where REVERSE_TIMESTAMP = 9999999999999 - (actual_ts_seconds * 1000)
        """
        # Batch layer uses underscore separator
        parts = rowkey.split('_')
        symbol = parts[0]
        reverse_ts = int(parts[1]) if len(parts) > 1 else 0
        
        # Convert reverse timestamp back to actual timestamp
        # reverse_ts = REVERSE_TIMESTAMP_MAX - (ts_seconds * 1000)
        # So: ts_seconds = (REVERSE_TIMESTAMP_MAX - reverse_ts) / 1000
        actual_ts_seconds = (REVERSE_TIMESTAMP_MAX - reverse_ts) // 1000
        
        record = {
            'rowkey': rowkey,
            'symbol': symbol,
            'window_end_ts': actual_ts_seconds,
            'window_end_time': datetime.fromtimestamp(actual_ts_seconds).isoformat(),
            'source': 'batch',
            'price_data': {},
            'bet_data': {},
            'analysis': {}
        }
        
        for key, value in data.items():
            cf, col = key.decode().split(':')
            # Map column family names to dict keys
            cf_key = cf
            if cf == 'price_data':
                cf_key = 'price_data'
            elif cf == 'bet_data':
                cf_key = 'bet_data'
            elif cf == 'analysis':
                cf_key = 'analysis'
            
            if cf_key in record:
                record[cf_key][col] = self._parse_value(value.decode())
        
        return record
    
    def _parse_live_record(self, rowkey: str, data: Dict) -> Dict:
        """
        Parse a speed layer HBase row into a structured dict.
        
        Speed layer RowKey format: TYPE#IDENTIFIER#REVERSE_TIMESTAMP
        where TYPE is BIN/PB/PT and REVERSE_TIMESTAMP = 9999999999999 - window_end_ms
        """
        parts = rowkey.split(ROWKEY_SEPARATOR)
        
        if len(parts) == 3:
            # Format: TYPE#IDENTIFIER#REVERSE_TS (speed layer windowed data)
            row_type, identifier, reverse_ts_str = parts
            reverse_ts = int(reverse_ts_str)
            actual_ts = REVERSE_TIMESTAMP_MAX - reverse_ts
            symbol = identifier
        elif len(parts) == 2:
            # Legacy format: SYMBOL#REVERSE_TS
            symbol, reverse_ts_str = parts
            reverse_ts = int(reverse_ts_str)
            actual_ts = REVERSE_TIMESTAMP_MAX - reverse_ts
            row_type = 'LEGACY'
        else:
            raise ValueError(f"Invalid rowkey format: {rowkey}")
        
        record = {
            'rowkey': rowkey,
            'symbol': symbol,
            'row_type': row_type if len(parts) == 3 else None,
            'timestamp': actual_ts,
            'timestamp_time': datetime.fromtimestamp(actual_ts / 1000).isoformat(),
            'source': 'speed',
            'data': {}
        }
        
        for key, value in data.items():
            _, col = key.decode().split(':')
            record['data'][col] = self._parse_value(value.decode())
        
        return record
    
    def _parse_value(self, value: str):
        """Parse a string value to appropriate type."""
        try:
            if '.' in value:
                return float(value)
            return int(value)
        except ValueError:
            return value
    
    def _merge_timelines(
        self,
        analytics: List[Dict],
        live: List[Dict],
        current_time: int
    ) -> List[Dict]:
        """
        Merge batch and speed layer data into a unified timeline.
        
        Strategy:
        - Use batch data for completed 15-min windows
        - Use speed data to fill in the current incomplete window
        - Prefer batch data when overlap exists (higher accuracy)
        
        Args:
            analytics: Batch layer records
            live: Speed layer records
            current_time: Current Unix timestamp (seconds)
            
        Returns:
            Merged timeline as list of dicts
        """
        timeline = []
        
        # Add all batch analytics (authoritative for completed windows)
        for record in analytics:
            timeline.append({
                'timestamp': record['window_end_ts'],
                'time': record['window_end_time'],
                'source': 'batch',
                'type': 'window_summary',
                'data': {
                    'price_data': record['price_data'],
                    'bet_data': record['bet_data'],
                    'analysis': record['analysis']
                }
            })
        
        # Find the most recent batch window end time
        last_batch_ts = max([r['window_end_ts'] for r in analytics]) if analytics else 0
        
        # Add live data only for timestamps after the last batch window
        for record in live:
            record_ts_seconds = record['timestamp'] // 1000
            if record_ts_seconds > last_batch_ts:
                timeline.append({
                    'timestamp': record_ts_seconds,
                    'time': record['timestamp_time'],
                    'source': 'speed',
                    'type': 'live_point',
                    'data': record['data']
                })
        
        # Sort by timestamp (newest first for dashboard display)
        timeline.sort(key=lambda x: x['timestamp'], reverse=True)
        
        return timeline
    
    # --------------------------------------------------------------------------
    # Dashboard-Ready Queries
    # --------------------------------------------------------------------------
    
    def get_current_state(self, symbol: str) -> Optional[Dict]:
        """
        Get the most current state for a symbol.
        
        Prioritizes speed layer for real-time data, falls back to batch.
        
        Args:
            symbol: Cryptocurrency symbol
            
        Returns:
            Current state dict or None
        """
        # Try speed layer first (most recent)
        live_records = self._query_speed_layer(symbol, limit=1)
        if live_records:
            return {
                'symbol': symbol,
                'source': 'speed',
                'timestamp': live_records[0]['timestamp'],
                'time': live_records[0]['timestamp_time'],
                **live_records[0]['data']
            }
        
        # Fall back to most recent batch window
        now = int(time.time())
        analytics = self._query_batch_layer(symbol, now - 3600, now)
        if analytics:
            latest = max(analytics, key=lambda x: x['window_end_ts'])
            return {
                'symbol': symbol,
                'source': 'batch',
                'timestamp': latest['window_end_ts'],
                'time': latest['window_end_time'],
                'price_data': latest['price_data'],
                'bet_data': latest['bet_data'],
                'analysis': latest['analysis']
            }
        
        return None
    
    def get_prediction_accuracy(
        self,
        symbol: str,
        window_count: int = 96  # 24 hours of 15-min windows
    ) -> Dict:
        """
        Calculate prediction accuracy for recent windows.
        
        Args:
            symbol: Cryptocurrency symbol
            window_count: Number of windows to analyze
            
        Returns:
            Dict with accuracy statistics
        """
        now = int(time.time())
        start_ts = now - (window_count * 15 * 60)  # 15 min per window
        
        analytics = self._query_batch_layer(symbol, start_ts, now)
        
        if not analytics:
            return {'symbol': symbol, 'windows_analyzed': 0, 'accuracy': None}
        
        correct = sum(1 for r in analytics if r['analysis'].get('result', '').startswith('CORRECT'))
        total = len(analytics)
        
        return {
            'symbol': symbol,
            'windows_analyzed': total,
            'correct_predictions': correct,
            'wrong_predictions': total - correct,
            'accuracy': correct / total if total > 0 else 0,
            'time_range': {
                'start': datetime.fromtimestamp(start_ts).isoformat(),
                'end': datetime.fromtimestamp(now).isoformat()
            }
        }


# ==============================================================================
# CLI Interface
# ==============================================================================

if __name__ == '__main__':
    import json
    
    print("Merge Engine - Lambda Architecture View Merger")
    print("=" * 50)
    
    if len(sys.argv) < 2:
        print("\nUsage:")
        print("  python merge_views.py <symbol> [--live] [--accuracy]")
        print("\nExamples:")
        print("  python merge_views.py BTC")
        print("  python merge_views.py ETH --live")
        print("  python merge_views.py BTC --accuracy")
        sys.exit(0)
    
    symbol = sys.argv[1].upper()
    
    try:
        with MergeEngine() as engine:
            if '--accuracy' in sys.argv:
                result = engine.get_prediction_accuracy(symbol)
                print(f"\nPrediction Accuracy for {symbol}:")
                print(json.dumps(result, indent=2))
            elif '--live' in sys.argv:
                result = engine.get_current_state(symbol)
                print(f"\nCurrent State for {symbol}:")
                print(json.dumps(result, indent=2))
            else:
                result = engine.get_merged_view(symbol)
                print(f"\nMerged View for {symbol}:")
                print(json.dumps(result, indent=2, default=str))
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
