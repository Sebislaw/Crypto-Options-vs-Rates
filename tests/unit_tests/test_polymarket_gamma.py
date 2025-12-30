"""
Unit tests for data_ingestion/collectors/polymarket_gamma.py
"""

import unittest
from unittest.mock import patch, Mock
import json
import sys
from pathlib import Path

# Add parent directories to path
parent_dir = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(parent_dir))

from ingestion_layer.polymarket.polymarket_gamma import (
    get_polymarket_events,
    get_current_15m_events,
    get_club_token_ids_from_15m_events
)


class TestGetPolymarketEvents(unittest.TestCase):
    """Test suite for get_polymarket_events function."""

    @patch('ingestion_layer.polymarket.polymarket_gamma.requests.get')
    def test_get_events_single_page(self, mock_get):
        """Test fetching events with single page response."""
        # Mock response data
        mock_events = [
            {'id': '1', 'title': 'Event 1'},
            {'id': '2', 'title': 'Event 2'},
        ]
        
        mock_response = Mock()
        mock_response.json.return_value = mock_events
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = get_polymarket_events(limit=10, paginate=False)
        
        self.assertEqual(result, mock_events)
        self.assertEqual(len(result), 2)
        mock_get.assert_called_once()

    @patch('ingestion_layer.polymarket.polymarket_gamma.requests.get')
    def test_get_events_multiple_pages(self, mock_get):
        """Test fetching events with pagination."""
        # Mock responses for multiple pages
        page1 = [{'id': str(i), 'title': f'Event {i}'} for i in range(1, 51)]
        page2 = [{'id': str(i), 'title': f'Event {i}'} for i in range(51, 76)]
        
        mock_response1 = Mock()
        mock_response1.json.return_value = page1
        mock_response1.raise_for_status.return_value = None
        
        mock_response2 = Mock()
        mock_response2.json.return_value = page2
        mock_response2.raise_for_status.return_value = None
        
        mock_get.side_effect = [mock_response1, mock_response2]
        
        result = get_polymarket_events(limit=50, paginate=True)
        
        self.assertEqual(len(result), 75)
        self.assertEqual(mock_get.call_count, 2)

    @patch('ingestion_layer.polymarket.polymarket_gamma.requests.get')
    def test_get_events_with_max_events(self, mock_get):
        """Test fetching events with max_events limit."""
        # Mock response with more events than max_events
        mock_events = [{'id': str(i), 'title': f'Event {i}'} for i in range(1, 51)]
        
        mock_response = Mock()
        mock_response.json.return_value = mock_events
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = get_polymarket_events(limit=50, max_events=25, paginate=True)
        
        self.assertEqual(len(result), 25)

    @patch('ingestion_layer.polymarket.polymarket_gamma.requests.get')
    def test_get_events_empty_response(self, mock_get):
        """Test handling of empty response."""
        mock_response = Mock()
        mock_response.json.return_value = []
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = get_polymarket_events()
        
        self.assertEqual(result, [])

    @patch('ingestion_layer.polymarket.polymarket_gamma.requests.get')
    def test_get_events_with_closed_filter_true(self, mock_get):
        """Test fetching closed events."""
        mock_events = [{'id': '1', 'closed': True}]
        
        mock_response = Mock()
        mock_response.json.return_value = mock_events
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = get_polymarket_events(closed=True, paginate=False)
        
        # Verify the closed parameter was passed correctly
        call_args = mock_get.call_args
        self.assertEqual(call_args[1]['params']['closed'], 'true')
        self.assertEqual(result, mock_events)

    @patch('ingestion_layer.polymarket.polymarket_gamma.requests.get')
    def test_get_events_with_closed_filter_false(self, mock_get):
        """Test fetching open events."""
        mock_events = [{'id': '1', 'closed': False}]
        
        mock_response = Mock()
        mock_response.json.return_value = mock_events
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = get_polymarket_events(closed=False, paginate=False)
        
        # Verify the closed parameter was passed correctly
        call_args = mock_get.call_args
        self.assertEqual(call_args[1]['params']['closed'], 'false')
        self.assertEqual(result, mock_events)

    @patch('ingestion_layer.polymarket.polymarket_gamma.requests.get')
    def test_get_events_with_closed_filter_none(self, mock_get):
        """Test fetching all events (no closed filter)."""
        mock_events = [{'id': '1'}, {'id': '2'}]
        
        mock_response = Mock()
        mock_response.json.return_value = mock_events
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = get_polymarket_events(closed=None, paginate=False)
        
        # Verify the closed parameter was not included
        call_args = mock_get.call_args
        self.assertNotIn('closed', call_args[1]['params'])
        self.assertEqual(result, mock_events)

    @patch('ingestion_layer.polymarket.polymarket_gamma.requests.get')
    def test_get_events_http_error(self, mock_get):
        """Test handling of HTTP errors."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("HTTP Error")
        mock_get.return_value = mock_response
        
        with self.assertRaises(Exception):
            get_polymarket_events()

    @patch('ingestion_layer.polymarket.polymarket_gamma.requests.get')
    @patch('builtins.print')
    def test_get_events_pagination_progress_print(self, mock_print, mock_get):
        """Test that pagination progress is printed."""
        page1 = [{'id': str(i)} for i in range(50)]
        page2 = [{'id': str(i)} for i in range(50, 75)]
        
        mock_response1 = Mock()
        mock_response1.json.return_value = page1
        mock_response1.raise_for_status.return_value = None
        
        mock_response2 = Mock()
        mock_response2.json.return_value = page2
        mock_response2.raise_for_status.return_value = None
        
        mock_get.side_effect = [mock_response1, mock_response2]
        
        get_polymarket_events(limit=50, paginate=True)
        
        # Verify progress was printed
        mock_print.assert_called()

    @patch('ingestion_layer.polymarket.polymarket_gamma.requests.get')
    def test_get_events_pagination_offset_increment(self, mock_get):
        """Test that offset is correctly incremented during pagination."""
        page1 = [{'id': str(i)} for i in range(50)]
        page2 = [{'id': str(i)} for i in range(50, 100)]
        page3 = [{'id': str(i)} for i in range(100, 120)]
        
        mock_response1 = Mock()
        mock_response1.json.return_value = page1
        mock_response1.raise_for_status.return_value = None
        
        mock_response2 = Mock()
        mock_response2.json.return_value = page2
        mock_response2.raise_for_status.return_value = None
        
        mock_response3 = Mock()
        mock_response3.json.return_value = page3
        mock_response3.raise_for_status.return_value = None
        
        mock_get.side_effect = [mock_response1, mock_response2, mock_response3]
        
        get_polymarket_events(limit=50, paginate=True)
        
        # Check that offsets were correct
        calls = mock_get.call_args_list
        self.assertEqual(calls[0][1]['params']['offset'], 0)
        self.assertEqual(calls[1][1]['params']['offset'], 50)
        self.assertEqual(calls[2][1]['params']['offset'], 100)


class TestGetCurrent15mEvents(unittest.TestCase):
    """Test suite for get_current_15m_events function."""

    @patch('ingestion_layer.polymarket.polymarket_gamma.current_quarter_timestamp_et')
    @patch('ingestion_layer.polymarket.polymarket_gamma.requests.get')
    def test_get_current_15m_events_success(self, mock_get, mock_timestamp):
        """Test successful fetching of 15-minute events."""
        mock_timestamp.return_value = 1234567890
        
        # Mock responses for each crypto
        btc_data = {'slug': 'btc-updown-15m-1234567890', 'markets': []}
        eth_data = {'slug': 'eth-updown-15m-1234567890', 'markets': []}
        sol_data = {'slug': 'sol-updown-15m-1234567890', 'markets': []}
        xrp_data = {'slug': 'xrp-updown-15m-1234567890', 'markets': []}
        
        mock_responses = [btc_data, eth_data, sol_data, xrp_data]
        
        mock_response = Mock()
        mock_response.json.side_effect = mock_responses
        mock_get.return_value = mock_response
        
        result = get_current_15m_events()
        
        # Verify all cryptos are in result
        self.assertIn('btc', result)
        self.assertIn('eth', result)
        self.assertIn('sol', result)
        self.assertIn('xrp', result)
        
        # Verify correct data
        self.assertEqual(result['btc'], btc_data)
        self.assertEqual(result['eth'], eth_data)
        self.assertEqual(result['sol'], sol_data)
        self.assertEqual(result['xrp'], xrp_data)
        
        # Verify correct number of requests
        self.assertEqual(mock_get.call_count, 4)

    @patch('ingestion_layer.polymarket.polymarket_gamma.current_quarter_timestamp_et')
    @patch('ingestion_layer.polymarket.polymarket_gamma.requests.get')
    def test_get_current_15m_events_url_construction(self, mock_get, mock_timestamp):
        """Test that URLs are constructed correctly."""
        mock_timestamp.return_value = 1700000000
        
        mock_response = Mock()
        mock_response.json.return_value = {}
        mock_get.return_value = mock_response
        
        get_current_15m_events()
        
        # Verify URLs
        calls = mock_get.call_args_list
        self.assertTrue(calls[0][0][0].endswith('btc-updown-15m-1700000000'))
        self.assertTrue(calls[1][0][0].endswith('eth-updown-15m-1700000000'))
        self.assertTrue(calls[2][0][0].endswith('sol-updown-15m-1700000000'))
        self.assertTrue(calls[3][0][0].endswith('xrp-updown-15m-1700000000'))

    @patch('ingestion_layer.polymarket.polymarket_gamma.current_quarter_timestamp_et')
    @patch('ingestion_layer.polymarket.polymarket_gamma.requests.get')
    def test_get_current_15m_events_http_error(self, mock_get, mock_timestamp):
        """Test handling of HTTP errors."""
        mock_timestamp.return_value = 1234567890
        
        mock_get.side_effect = Exception("Connection error")
        
        with self.assertRaises(Exception):
            get_current_15m_events()


class TestGetClubTokenIdsFrom15mEvents(unittest.TestCase):
    """Test suite for get_club_token_ids_from_15m_events function."""

    @patch('ingestion_layer.polymarket.polymarket_gamma.get_current_15m_events')
    def test_get_club_token_ids_success(self, mock_get_events):
        """Test successful extraction of CLOB token IDs."""
        # Mock event data with JSON-encoded clobTokenIds
        mock_events = {
            'btc': {
                'markets': [
                    {'clobTokenIds': '["btc-token-1", "btc-token-2"]'}
                ]
            },
            'eth': {
                'markets': [
                    {'clobTokenIds': '["eth-token-1", "eth-token-2"]'}
                ]
            },
            'sol': {
                'markets': [
                    {'clobTokenIds': '["sol-token-1", "sol-token-2"]'}
                ]
            },
            'xrp': {
                'markets': [
                    {'clobTokenIds': '["xrp-token-1", "xrp-token-2"]'}
                ]
            }
        }
        
        mock_get_events.return_value = mock_events
        
        result = get_club_token_ids_from_15m_events()
        
        # Verify structure
        self.assertIn('btc', result)
        self.assertIn('eth', result)
        self.assertIn('sol', result)
        self.assertIn('xrp', result)
        
        # Verify token IDs are correctly parsed
        self.assertEqual(result['btc'], ['btc-token-1', 'btc-token-2'])
        self.assertEqual(result['eth'], ['eth-token-1', 'eth-token-2'])
        self.assertEqual(result['sol'], ['sol-token-1', 'sol-token-2'])
        self.assertEqual(result['xrp'], ['xrp-token-1', 'xrp-token-2'])

    @patch('ingestion_layer.polymarket.polymarket_gamma.get_current_15m_events')
    def test_get_club_token_ids_empty_token_list(self, mock_get_events):
        """Test handling of empty token lists."""
        mock_events = {
            'btc': {'markets': [{'clobTokenIds': '[]'}]},
            'eth': {'markets': [{'clobTokenIds': '[]'}]},
            'sol': {'markets': [{'clobTokenIds': '[]'}]},
            'xrp': {'markets': [{'clobTokenIds': '[]'}]}
        }
        
        mock_get_events.return_value = mock_events
        
        result = get_club_token_ids_from_15m_events()
        
        self.assertEqual(result['btc'], [])
        self.assertEqual(result['eth'], [])
        self.assertEqual(result['sol'], [])
        self.assertEqual(result['xrp'], [])

    @patch('ingestion_layer.polymarket.polymarket_gamma.get_current_15m_events')
    def test_get_club_token_ids_single_token(self, mock_get_events):
        """Test extraction with single token per crypto."""
        mock_events = {
            'btc': {'markets': [{'clobTokenIds': '["btc-single"]'}]},
            'eth': {'markets': [{'clobTokenIds': '["eth-single"]'}]},
            'sol': {'markets': [{'clobTokenIds': '["sol-single"]'}]},
            'xrp': {'markets': [{'clobTokenIds': '["xrp-single"]'}]}
        }
        
        mock_get_events.return_value = mock_events
        
        result = get_club_token_ids_from_15m_events()
        
        self.assertEqual(result['btc'], ['btc-single'])
        self.assertEqual(result['eth'], ['eth-single'])
        self.assertEqual(result['sol'], ['sol-single'])
        self.assertEqual(result['xrp'], ['xrp-single'])

    @patch('ingestion_layer.polymarket.polymarket_gamma.get_current_15m_events')
    def test_get_club_token_ids_invalid_json(self, mock_get_events):
        """Test handling of invalid JSON in clobTokenIds."""
        mock_events = {
            'btc': {'markets': [{'clobTokenIds': 'invalid-json'}]},
            'eth': {'markets': [{'clobTokenIds': '[]'}]},
            'sol': {'markets': [{'clobTokenIds': '[]'}]},
            'xrp': {'markets': [{'clobTokenIds': '[]'}]}
        }
        
        mock_get_events.return_value = mock_events
        
        with self.assertRaises(json.JSONDecodeError):
            get_club_token_ids_from_15m_events()

    @patch('ingestion_layer.polymarket.polymarket_gamma.get_current_15m_events')
    def test_get_club_token_ids_missing_markets_key(self, mock_get_events):
        """Test handling of missing 'markets' key."""
        mock_events = {
            'btc': {},
            'eth': {'markets': []},
            'sol': {'markets': []},
            'xrp': {'markets': []}
        }
        
        mock_get_events.return_value = mock_events
        
        with self.assertRaises(KeyError):
            get_club_token_ids_from_15m_events()

    @patch('ingestion_layer.polymarket.polymarket_gamma.get_current_15m_events')
    def test_get_club_token_ids_empty_markets_list(self, mock_get_events):
        """Test handling of empty markets list."""
        mock_events = {
            'btc': {'markets': []},
            'eth': {'markets': []},
            'sol': {'markets': []},
            'xrp': {'markets': []}
        }
        
        mock_get_events.return_value = mock_events
        
        with self.assertRaises(IndexError):
            get_club_token_ids_from_15m_events()


if __name__ == '__main__':
    unittest.main()
