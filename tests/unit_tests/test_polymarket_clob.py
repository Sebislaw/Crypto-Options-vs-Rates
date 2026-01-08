"""
Unit tests for data_ingestion/collectors/polymarket_clob.py
"""

import unittest
from unittest.mock import patch, Mock
import json
import sys
from pathlib import Path

# Add parent directories to path
parent_dir = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(parent_dir))

from ingestion_layer.polymarket.polymarket_clob import (
    WebSocketOrderBook,
    start_market_websocket_connection
)


class TestWebSocketOrderBook(unittest.TestCase):
    """Test suite for WebSocketOrderBook class."""

    def setUp(self):
        """Set up test fixtures."""
        self.channel_type = "market"
        self.url = "wss://test.example.com"
        self.data = ["token-1", "token-2"]
        self.message_callback = Mock()

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketApp')
    @patch('ingestion_layer.polymarket.polymarket_clob.current_quarter_timestamp_et')
    def test_init_creates_websocket(self, mock_timestamp, mock_ws_app):
        """Test that WebSocketOrderBook initializes correctly."""
        mock_timestamp.return_value = 1234567890
        
        ws_orderbook = WebSocketOrderBook(
            self.channel_type,
            self.url,
            self.data,
            self.message_callback,
            verbose=True,
            crypto_name="btc"
        )
        
        # Verify attributes
        self.assertEqual(ws_orderbook.channel_type, "market")
        self.assertEqual(ws_orderbook.url, self.url)
        self.assertEqual(ws_orderbook.data, self.data)
        self.assertEqual(ws_orderbook.message_callback, self.message_callback)
        self.assertTrue(ws_orderbook.verbose)
        self.assertEqual(ws_orderbook.crypto_name, "btc")
        self.assertEqual(ws_orderbook.current_timestamp, 1234567890)
        self.assertTrue(ws_orderbook.should_reconnect)
        
        # Verify WebSocketApp was created with correct URL
        expected_url = self.url + "/ws/" + self.channel_type
        mock_ws_app.assert_called_once()
        call_args = mock_ws_app.call_args
        self.assertEqual(call_args[0][0], expected_url)

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketApp')
    @patch('ingestion_layer.polymarket.polymarket_clob.current_quarter_timestamp_et')
    def test_init_without_optional_params(self, mock_timestamp, mock_ws_app):
        """Test initialization without optional parameters."""
        mock_timestamp.return_value = 1234567890
        
        ws_orderbook = WebSocketOrderBook(
            self.channel_type,
            self.url,
            self.data,
            self.message_callback
        )
        
        self.assertFalse(ws_orderbook.verbose)
        self.assertIsNone(ws_orderbook.crypto_name)

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketApp')
    @patch('ingestion_layer.polymarket.polymarket_clob.current_quarter_timestamp_et')
    @patch('builtins.print')
    def test_on_message(self, mock_print, mock_timestamp, mock_ws_app):
        """Test on_message callback."""
        mock_timestamp.return_value = 1234567890
        
        ws_orderbook = WebSocketOrderBook(
            self.channel_type,
            self.url,
            self.data,
            self.message_callback
        )
        
        mock_ws = Mock()
        test_message = '{"type": "test", "data": "value"}'
        
        ws_orderbook.on_message(mock_ws, test_message)
        
        # Verify message was printed
        mock_print.assert_called_once_with(test_message)

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketApp')
    @patch('ingestion_layer.polymarket.polymarket_clob.current_quarter_timestamp_et')
    @patch('builtins.print')
    def test_on_error(self, mock_print, mock_timestamp, mock_ws_app):
        """Test on_error callback."""
        mock_timestamp.return_value = 1234567890
        
        ws_orderbook = WebSocketOrderBook(
            self.channel_type,
            self.url,
            self.data,
            self.message_callback
        )
        
        mock_ws = Mock()
        test_error = "Connection timeout"
        
        with self.assertRaises(SystemExit) as cm:
            ws_orderbook.on_error(mock_ws, test_error)
        
        self.assertEqual(cm.exception.code, 1)
        mock_print.assert_called_once_with("Error: ", test_error)

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketApp')
    @patch('ingestion_layer.polymarket.polymarket_clob.current_quarter_timestamp_et')
    @patch('builtins.print')
    def test_on_close_with_reconnect(self, mock_print, mock_timestamp, mock_ws_app):
        """Test on_close callback when reconnection is allowed."""
        mock_timestamp.return_value = 1234567890
        
        ws_orderbook = WebSocketOrderBook(
            self.channel_type,
            self.url,
            self.data,
            self.message_callback
        )
        
        ws_orderbook.should_reconnect = True
        mock_ws = Mock()
        
        # Should not exit when should_reconnect is True
        ws_orderbook.on_close(mock_ws, 1000, "Normal closure")
        
        mock_print.assert_called_once_with("Closing connection (status: 1000, msg: Normal closure)")

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketApp')
    @patch('ingestion_layer.polymarket.polymarket_clob.current_quarter_timestamp_et')
    @patch('builtins.print')
    def test_on_close_without_reconnect(self, mock_print, mock_timestamp, mock_ws_app):
        """Test on_close callback when reconnection is not allowed."""
        mock_timestamp.return_value = 1234567890
        
        ws_orderbook = WebSocketOrderBook(
            self.channel_type,
            self.url,
            self.data,
            self.message_callback
        )
        
        ws_orderbook.should_reconnect = False
        mock_ws = Mock()
        
        with self.assertRaises(SystemExit) as cm:
            ws_orderbook.on_close(mock_ws, 1000, "Normal closure")
        
        self.assertEqual(cm.exception.code, 0)

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketApp')
    @patch('ingestion_layer.polymarket.polymarket_clob.current_quarter_timestamp_et')
    @patch('ingestion_layer.polymarket.polymarket_clob.threading.Thread')
    @patch('builtins.print')
    def test_on_open_market_channel(self, mock_print, mock_thread, mock_timestamp, mock_ws_app):
        """Test on_open callback for market channel."""
        mock_timestamp.return_value = 1234567890
        
        ws_orderbook = WebSocketOrderBook(
            "market",
            self.url,
            self.data,
            self.message_callback
        )
        
        mock_ws = Mock()
        ws_orderbook.on_open(mock_ws)
        
        # Verify subscription message was sent
        expected_message = json.dumps({"assets_ids": self.data, "type": "market"})
        mock_ws.send.assert_called_once_with(expected_message)
        
        # Verify print was called
        mock_print.assert_called()
        
        # Verify threads were started
        self.assertEqual(mock_thread.call_count, 2)  # ping and monitor threads

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketApp')
    @patch('ingestion_layer.polymarket.polymarket_clob.current_quarter_timestamp_et')
    def test_on_open_invalid_channel(self, mock_timestamp, mock_ws_app):
        """Test on_open callback with invalid channel type."""
        mock_timestamp.return_value = 1234567890
        
        ws_orderbook = WebSocketOrderBook(
            "invalid",
            self.url,
            self.data,
            self.message_callback
        )
        
        mock_ws = Mock()
        
        with self.assertRaises(SystemExit) as cm:
            ws_orderbook.on_open(mock_ws)
        
        self.assertEqual(cm.exception.code, 1)

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketApp')
    @patch('ingestion_layer.polymarket.polymarket_clob.current_quarter_timestamp_et')
    @patch('ingestion_layer.polymarket.polymarket_clob.time.sleep')
    def test_ping(self, mock_sleep, mock_timestamp, mock_ws_app):
        """Test ping method sends periodic ping messages."""
        mock_timestamp.return_value = 1234567890
        
        ws_orderbook = WebSocketOrderBook(
            self.channel_type,
            self.url,
            self.data,
            self.message_callback
        )
        
        mock_ws = Mock()
        
        # Mock sleep to raise exception after 3 iterations to break the loop
        call_count = [0]
        def side_effect(duration):
            call_count[0] += 1
            if call_count[0] >= 3:
                raise KeyboardInterrupt()
        
        mock_sleep.side_effect = side_effect
        
        with self.assertRaises(KeyboardInterrupt):
            ws_orderbook.ping(mock_ws)
        
        # Verify PING was sent multiple times
        self.assertEqual(mock_ws.send.call_count, 3)
        for call_args in mock_ws.send.call_args_list:
            self.assertEqual(call_args[0][0], "PING")

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketApp')
    @patch('ingestion_layer.polymarket.polymarket_clob.current_quarter_timestamp_et')
    @patch('ingestion_layer.polymarket.polymarket_clob.time.sleep')
    @patch('ingestion_layer.polymarket.polymarket_clob.time.time')
    @patch('builtins.print')
    def test_monitor_timestamp(self, mock_print, mock_time, mock_sleep, mock_timestamp, mock_ws_app):
        """Test monitor_timestamp closes connection at next 15-minute window."""
        current_ts = 1234567890
        mock_timestamp.return_value = current_ts
        mock_time.return_value = current_ts
        
        ws_orderbook = WebSocketOrderBook(
            self.channel_type,
            self.url,
            self.data,
            self.message_callback
        )
        
        mock_ws = Mock()
        
        ws_orderbook.monitor_timestamp(mock_ws)
        
        # Verify sleep was called with 15 minutes (900 seconds)
        mock_sleep.assert_called_once_with(15 * 60)
        
        # Verify close was called
        mock_ws.close.assert_called_once()
        
        # Verify print messages
        self.assertEqual(mock_print.call_count, 2)

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketApp')
    @patch('ingestion_layer.polymarket.polymarket_clob.current_quarter_timestamp_et')
    @patch('ingestion_layer.polymarket.polymarket_clob.time.sleep')
    @patch('ingestion_layer.polymarket.polymarket_clob.time.time')
    def test_monitor_timestamp_negative_sleep(self, mock_time, mock_sleep, mock_timestamp, mock_ws_app):
        """Test monitor_timestamp when already past next window."""
        current_ts = 1234567890
        mock_timestamp.return_value = current_ts
        # Simulate time has already passed the next window
        mock_time.return_value = current_ts + (20 * 60)  # 20 minutes later
        
        ws_orderbook = WebSocketOrderBook(
            self.channel_type,
            self.url,
            self.data,
            self.message_callback
        )
        
        mock_ws = Mock()
        
        ws_orderbook.monitor_timestamp(mock_ws)
        
        # Verify sleep was not called for negative duration
        mock_sleep.assert_not_called()
        
        # Verify close was still called
        mock_ws.close.assert_called_once()

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketApp')
    @patch('ingestion_layer.polymarket.polymarket_clob.current_quarter_timestamp_et')
    def test_run(self, mock_timestamp, mock_ws_app):
        """Test run method calls ws.run_forever."""
        mock_timestamp.return_value = 1234567890
        
        ws_orderbook = WebSocketOrderBook(
            self.channel_type,
            self.url,
            self.data,
            self.message_callback
        )
        
        ws_orderbook.run()
        
        # Verify run_forever was called
        ws_orderbook.ws.run_forever.assert_called_once()


class TestStartMarketWebSocketConnection(unittest.TestCase):
    """Test suite for start_market_websocket_connection function."""

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketOrderBook')
    @patch('ingestion_layer.polymarket.polymarket_clob.get_club_token_ids_from_15m_events')
    @patch('builtins.print')
    def test_successful_connection(self, mock_print, mock_get_tokens, mock_ws_class):
        """Test successful WebSocket connection setup."""
        # Mock token data
        mock_get_tokens.return_value = {
            'btc': ['btc-token-1', 'btc-token-2'],
            'eth': ['eth-token-1'],
            'sol': ['sol-token-1'],
            'xrp': ['xrp-token-1']
        }
        
        # Mock WebSocket instance
        mock_ws_instance = Mock()
        mock_ws_class.return_value = mock_ws_instance
        
        # Make run() raise KeyboardInterrupt to exit the loop
        mock_ws_instance.run.side_effect = KeyboardInterrupt()
        
        start_market_websocket_connection('btc')
        
        # Verify tokens were fetched
        mock_get_tokens.assert_called_once()
        
        # Verify WebSocket was created with correct parameters
        mock_ws_class.assert_called_once()
        call_args = mock_ws_class.call_args
        self.assertEqual(call_args[0][0], "market")  # channel_type
        self.assertEqual(call_args[0][2], ['btc-token-1'])  # token_ids (only first one)
        self.assertEqual(call_args[0][5], 'btc')  # crypto_name (6th positional arg)
        
        # Verify run was called
        mock_ws_instance.run.assert_called_once()

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketOrderBook')
    @patch('ingestion_layer.polymarket.polymarket_clob.get_club_token_ids_from_15m_events')
    @patch('ingestion_layer.polymarket.polymarket_clob.time.sleep')
    @patch('builtins.print')
    def test_crypto_not_found(self, mock_print, mock_sleep, mock_get_tokens, mock_ws_class):
        """Test handling when crypto name is not found in tokens."""
        # Mock token data without the requested crypto
        mock_get_tokens.return_value = {
            'eth': ['eth-token-1'],
            'sol': ['sol-token-1']
        }
        
        # Make sleep raise KeyboardInterrupt after first call to exit loop
        mock_sleep.side_effect = KeyboardInterrupt()
        
        start_market_websocket_connection('btc')
        
        # Verify error message was printed
        mock_print.assert_any_call("Error: No tokens found for btc")
        
        # Verify sleep was called (60 seconds wait)
        mock_sleep.assert_called_once_with(60)
        
        # Verify WebSocket was never created
        mock_ws_class.assert_not_called()

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketOrderBook')
    @patch('ingestion_layer.polymarket.polymarket_clob.get_club_token_ids_from_15m_events')
    @patch('ingestion_layer.polymarket.polymarket_clob.time.sleep')
    @patch('builtins.print')
    def test_exception_handling(self, mock_print, mock_sleep, mock_get_tokens, mock_ws_class):
        """Test exception handling and retry mechanism."""
        # Make get_tokens raise an exception first, then KeyboardInterrupt
        mock_get_tokens.side_effect = [
            Exception("Network error"),
            KeyboardInterrupt()
        ]
        
        start_market_websocket_connection('btc')
        
        # Verify error message was printed
        mock_print.assert_any_call("Error occurred: Network error")
        mock_print.assert_any_call("Waiting 10 seconds before retrying...")
        
        # Verify sleep was called with 10 seconds
        mock_sleep.assert_called_once_with(10)

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketOrderBook')
    @patch('ingestion_layer.polymarket.polymarket_clob.get_club_token_ids_from_15m_events')
    @patch('builtins.print')
    def test_keyboard_interrupt(self, mock_print, mock_get_tokens, mock_ws_class):
        """Test graceful shutdown on KeyboardInterrupt."""
        mock_get_tokens.side_effect = KeyboardInterrupt()
        
        start_market_websocket_connection('btc')
        
        # Verify shutdown message
        mock_print.assert_any_call("\nShutting down...")

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketOrderBook')
    @patch('ingestion_layer.polymarket.polymarket_clob.get_club_token_ids_from_15m_events')
    @patch('builtins.print')
    def test_multiple_reconnections(self, mock_print, mock_get_tokens, mock_ws_class):
        """Test that the function reconnects after connection closes."""
        mock_get_tokens.return_value = {
            'btc': ['btc-token-1', 'btc-token-2'],
        }
        
        # Mock WebSocket to complete normally twice, then raise KeyboardInterrupt
        mock_ws_instance1 = Mock()
        mock_ws_instance2 = Mock()
        mock_ws_instance2.run.side_effect = KeyboardInterrupt()
        
        mock_ws_class.side_effect = [mock_ws_instance1, mock_ws_instance2]
        
        start_market_websocket_connection('btc')
        
        # Verify WebSocket was created twice (reconnected)
        self.assertEqual(mock_ws_class.call_count, 2)
        
        # Verify tokens were fetched twice
        self.assertEqual(mock_get_tokens.call_count, 2)

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketOrderBook')
    @patch('ingestion_layer.polymarket.polymarket_clob.get_club_token_ids_from_15m_events')
    @patch('builtins.print')
    def test_uses_first_token_only(self, mock_print, mock_get_tokens, mock_ws_class):
        """Test that only the first token is used from the list."""
        mock_get_tokens.return_value = {
            'eth': ['eth-token-1', 'eth-token-2', 'eth-token-3'],
        }
        
        mock_ws_instance = Mock()
        mock_ws_instance.run.side_effect = KeyboardInterrupt()
        mock_ws_class.return_value = mock_ws_instance
        
        start_market_websocket_connection('eth')
        
        # Verify only first token was used
        call_args = mock_ws_class.call_args
        self.assertEqual(call_args[0][2], ['eth-token-1'])
        self.assertEqual(len(call_args[0][2]), 1)

    @patch('ingestion_layer.polymarket.polymarket_clob.WebSocketOrderBook')
    @patch('ingestion_layer.polymarket.polymarket_clob.get_club_token_ids_from_15m_events')
    @patch('builtins.print')
    def test_different_crypto_names(self, mock_print, mock_get_tokens, mock_ws_class):
        """Test connection with different cryptocurrency names."""
        for crypto in ['btc', 'eth', 'sol', 'xrp']:
            mock_get_tokens.reset_mock()
            mock_ws_class.reset_mock()
            
            mock_get_tokens.return_value = {
                crypto: [f'{crypto}-token-1'],
            }
            
            mock_ws_instance = Mock()
            mock_ws_instance.run.side_effect = KeyboardInterrupt()
            mock_ws_class.return_value = mock_ws_instance
            
            start_market_websocket_connection(crypto)
            
            # Verify crypto_name was passed correctly
            call_args = mock_ws_class.call_args
            self.assertEqual(call_args[0][5], crypto)  # crypto_name (6th positional arg)


if __name__ == '__main__':
    unittest.main()
