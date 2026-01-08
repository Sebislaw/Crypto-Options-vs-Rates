"""
Unit tests for data_ingestion/utils/time_utils.py
"""

import unittest
from unittest.mock import patch
from datetime import datetime as real_datetime
import pytz
import sys
from pathlib import Path

# Add parent directories to path
parent_dir = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(parent_dir))

from ingestion_layer.polymarket.utils.time_utils import current_quarter_timestamp_et


class TestCurrentQuarterTimestampET(unittest.TestCase):
    """Test suite for current_quarter_timestamp_et function."""

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_quarter_hour_00_minutes(self, mock_datetime):
        """Test timestamp calculation at exactly 00 minutes (start of hour)."""
        # Mock datetime: 2024-01-15 10:00:30.123456 ET
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 1, 15, 10, 0, 30, 123456))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-01-15 10:00:00 ET -> UTC
        expected_et = eastern.localize(real_datetime(2024, 1, 15, 10, 0, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)
        self.assertIsInstance(result, int)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_quarter_hour_15_minutes(self, mock_datetime):
        """Test timestamp calculation at exactly 15 minutes."""
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 1, 15, 10, 15, 45, 999999))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-01-15 10:15:00 ET -> UTC
        expected_et = eastern.localize(real_datetime(2024, 1, 15, 10, 15, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_quarter_hour_30_minutes(self, mock_datetime):
        """Test timestamp calculation at exactly 30 minutes."""
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 1, 15, 10, 30, 0, 0))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-01-15 10:30:00 ET -> UTC
        expected_et = eastern.localize(real_datetime(2024, 1, 15, 10, 30, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_quarter_hour_45_minutes(self, mock_datetime):
        """Test timestamp calculation at exactly 45 minutes."""
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 1, 15, 10, 45, 30, 123456))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-01-15 10:45:00 ET -> UTC
        expected_et = eastern.localize(real_datetime(2024, 1, 15, 10, 45, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_rounds_down_from_01_minute(self, mock_datetime):
        """Test that 01 minutes rounds down to 00."""
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 1, 15, 10, 1, 30, 0))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-01-15 10:00:00 ET
        expected_et = eastern.localize(real_datetime(2024, 1, 15, 10, 0, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_rounds_down_from_14_minutes(self, mock_datetime):
        """Test that 14 minutes rounds down to 00."""
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 1, 15, 10, 14, 59, 999999))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-01-15 10:00:00 ET
        expected_et = eastern.localize(real_datetime(2024, 1, 15, 10, 0, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_rounds_down_from_16_minutes(self, mock_datetime):
        """Test that 16 minutes rounds down to 15."""
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 1, 15, 10, 16, 30, 0))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-01-15 10:15:00 ET
        expected_et = eastern.localize(real_datetime(2024, 1, 15, 10, 15, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_rounds_down_from_29_minutes(self, mock_datetime):
        """Test that 29 minutes rounds down to 15."""
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 1, 15, 10, 29, 59, 999999))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-01-15 10:15:00 ET
        expected_et = eastern.localize(real_datetime(2024, 1, 15, 10, 15, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_rounds_down_from_31_minutes(self, mock_datetime):
        """Test that 31 minutes rounds down to 30."""
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 1, 15, 10, 31, 0, 0))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-01-15 10:30:00 ET
        expected_et = eastern.localize(real_datetime(2024, 1, 15, 10, 30, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_rounds_down_from_44_minutes(self, mock_datetime):
        """Test that 44 minutes rounds down to 30."""
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 1, 15, 10, 44, 59, 999999))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-01-15 10:30:00 ET
        expected_et = eastern.localize(real_datetime(2024, 1, 15, 10, 30, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_rounds_down_from_46_minutes(self, mock_datetime):
        """Test that 46 minutes rounds down to 45."""
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 1, 15, 10, 46, 30, 0))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-01-15 10:45:00 ET
        expected_et = eastern.localize(real_datetime(2024, 1, 15, 10, 45, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_rounds_down_from_59_minutes(self, mock_datetime):
        """Test that 59 minutes rounds down to 45."""
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 1, 15, 10, 59, 59, 999999))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-01-15 10:45:00 ET
        expected_et = eastern.localize(real_datetime(2024, 1, 15, 10, 45, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_midnight(self, mock_datetime):
        """Test timestamp calculation at midnight."""
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 1, 15, 0, 5, 30, 0))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-01-15 00:00:00 ET
        expected_et = eastern.localize(real_datetime(2024, 1, 15, 0, 0, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_near_midnight(self, mock_datetime):
        """Test timestamp calculation near midnight (11:50 PM)."""
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 1, 15, 23, 50, 30, 0))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-01-15 23:45:00 ET
        expected_et = eastern.localize(real_datetime(2024, 1, 15, 23, 45, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_dst_transition_spring(self, mock_datetime):
        """Test during DST transition in spring (clocks spring forward)."""
        # March 10, 2024, 2:30 AM EST becomes 3:30 AM EDT
        eastern = pytz.timezone("America/New_York")
        # Use a time after the transition
        mock_now = eastern.localize(real_datetime(2024, 3, 10, 3, 20, 0, 0))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-03-10 03:15:00 EDT
        expected_et = eastern.localize(real_datetime(2024, 3, 10, 3, 15, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_dst_transition_fall(self, mock_datetime):
        """Test during DST transition in fall (clocks fall back)."""
        # November 3, 2024, 2:00 AM EDT becomes 1:00 AM EST
        eastern = pytz.timezone("America/New_York")
        # Use a time after the transition
        mock_now = eastern.localize(real_datetime(2024, 11, 3, 1, 35, 0, 0))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-11-03 01:30:00 EST
        expected_et = eastern.localize(real_datetime(2024, 11, 3, 1, 30, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_summer_time(self, mock_datetime):
        """Test during summer (EDT - Eastern Daylight Time)."""
        eastern = pytz.timezone("America/New_York")
        # July is definitely EDT
        mock_now = eastern.localize(real_datetime(2024, 7, 15, 14, 22, 30, 0))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-07-15 14:15:00 EDT
        expected_et = eastern.localize(real_datetime(2024, 7, 15, 14, 15, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_winter_time(self, mock_datetime):
        """Test during winter (EST - Eastern Standard Time)."""
        eastern = pytz.timezone("America/New_York")
        # January is definitely EST
        mock_now = eastern.localize(real_datetime(2024, 1, 15, 14, 22, 30, 0))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-01-15 14:15:00 EST
        expected_et = eastern.localize(real_datetime(2024, 1, 15, 14, 15, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_return_type_is_integer(self, mock_datetime):
        """Test that the function returns an integer."""
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 1, 15, 10, 23, 30, 500000))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        self.assertIsInstance(result, int)
        # Verify no decimal part
        self.assertEqual(result, int(result))

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_seconds_are_zeroed(self, mock_datetime):
        """Test that seconds are always set to 0 in the result."""
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 1, 15, 10, 30, 45, 123456))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-01-15 10:30:00 ET (45 seconds should be zeroed)
        expected_et = eastern.localize(real_datetime(2024, 1, 15, 10, 30, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_microseconds_are_zeroed(self, mock_datetime):
        """Test that microseconds are always set to 0 in the result."""
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 1, 15, 10, 15, 0, 999999))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-01-15 10:15:00.000000 ET
        expected_et = eastern.localize(real_datetime(2024, 1, 15, 10, 15, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_new_years_eve(self, mock_datetime):
        """Test timestamp calculation on New Year's Eve."""
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 12, 31, 23, 58, 30, 0))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-12-31 23:45:00 ET
        expected_et = eastern.localize(real_datetime(2024, 12, 31, 23, 45, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_leap_year_feb_29(self, mock_datetime):
        """Test timestamp calculation on leap year February 29."""
        eastern = pytz.timezone("America/New_York")
        mock_now = eastern.localize(real_datetime(2024, 2, 29, 12, 37, 15, 0))
        mock_datetime.now.return_value = mock_now
        
        result = current_quarter_timestamp_et()
        
        # Expected: 2024-02-29 12:30:00 ET
        expected_et = eastern.localize(real_datetime(2024, 2, 29, 12, 30, 0, 0))
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        self.assertEqual(result, expected_timestamp)

    def test_real_time_execution(self):
        """Test that the function executes without errors with real datetime."""
        # This test uses actual datetime (no mocking)
        result = current_quarter_timestamp_et()
        
        # Verify it returns an integer
        self.assertIsInstance(result, int)
        
        # Verify it's a reasonable timestamp (after 2020 and before 2100)
        timestamp_2020 = 1577836800  # 2020-01-01 00:00:00 UTC
        timestamp_2100 = 4102444800  # 2100-01-01 00:00:00 UTC
        self.assertGreater(result, timestamp_2020)
        self.assertLess(result, timestamp_2100)
        
        # Verify the timestamp corresponds to a time with minutes divisible by 15
        eastern = pytz.timezone("America/New_York")
        dt_utc = real_datetime.fromtimestamp(result, tz=pytz.UTC)
        dt_et = dt_utc.astimezone(eastern)
        self.assertIn(dt_et.minute, [0, 15, 30, 45])
        self.assertEqual(dt_et.second, 0)
        self.assertEqual(dt_et.microsecond, 0)

    @patch('ingestion_layer.polymarket.utils.time_utils.datetime.datetime')
    def test_consistent_results_within_same_quarter(self, mock_datetime):
        """Test that all times within the same 15-min block return the same timestamp."""
        eastern = pytz.timezone("America/New_York")
        
        # Test all times between 10:30:00 and 10:44:59
        base_date = real_datetime(2024, 1, 15, 10, 30, 0, 0)
        expected_et = eastern.localize(base_date)
        expected_utc = expected_et.astimezone(pytz.UTC)
        expected_timestamp = int(expected_utc.timestamp())
        
        # Test a few different times in the same quarter
        test_times = [
            (30, 0, 0),      # Exact start
            (30, 30, 0),     # Middle
            (35, 15, 123456),  # Random time
            (40, 0, 0),      # 10 minutes in
            (44, 59, 999999),  # Last moment
        ]
        
        for minute, second, microsecond in test_times:
            mock_now = eastern.localize(real_datetime(2024, 1, 15, 10, minute, second, microsecond))
            mock_datetime.now.return_value = mock_now
            
            result = current_quarter_timestamp_et()
            self.assertEqual(result, expected_timestamp, 
                           f"Failed for time 10:{minute:02d}:{second:02d}.{microsecond:06d}")


if __name__ == '__main__':
    unittest.main()
