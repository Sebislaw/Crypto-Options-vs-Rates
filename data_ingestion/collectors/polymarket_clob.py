from py_clob_client.client import ClobClient
from websocket import WebSocketApp
import json
import time
import threading
import sys
from pathlib import Path
from typing import List

parent_dir = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(parent_dir))

from data_ingestion.collectors.polymarket_gamma import get_club_token_ids_from_15m_events
from configs.polymarket_config import (
    CLOB_API_BASE_URL,
    MARKET_CHANNEL,
    WEBSOCKET_URL,
    WEBSOCKET_PING_INTERVAL
)
from data_ingestion.utils.time_utils import current_quarter_timestamp_et


client = ClobClient(CLOB_API_BASE_URL)  # Level 0 (no auth)


class WebSocketOrderBook:
    def __init__(self, channel_type: str, url: str, data: List[str], message_callback: callable, verbose: bool = False, crypto_name: str = None):
        self.channel_type = channel_type
        self.url = url
        self.data = data
        self.message_callback = message_callback
        self.verbose = verbose
        self.crypto_name = crypto_name
        self.current_timestamp = current_quarter_timestamp_et()
        self.should_reconnect = True
        furl = url + "/ws/" + channel_type
        self.ws = WebSocketApp(
            furl,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )
        self.orderbooks = {}

    def on_message(self, ws: WebSocketApp, message: str):
        print(message)
        pass

    def on_error(self, ws: WebSocketApp, error: str):
        print("Error: ", error)
        exit(1)

    def on_close(self, ws: WebSocketApp, close_status_code: int, close_msg: str):
        print(f"Closing connection (status: {close_status_code}, msg: {close_msg})")
        if not self.should_reconnect:
            exit(0)

    def on_open(self, ws: WebSocketApp):
        if self.channel_type == MARKET_CHANNEL:
            ws.send(json.dumps({"assets_ids": self.data, "type": MARKET_CHANNEL}))
            print(f"Connected to market with {len(self.data)} token IDs at timestamp {self.current_timestamp}")
        else:
            exit(1)

        # Start ping thread
        ping_thread = threading.Thread(target=self.ping, args=(ws,))
        ping_thread.daemon = True
        ping_thread.start()
        
        # Start timestamp monitor thread
        monitor_thread = threading.Thread(target=self.monitor_timestamp, args=(ws,))
        monitor_thread.daemon = True
        monitor_thread.start()

    def ping(self, ws: WebSocketApp):
        while True:
            ws.send("PING")
            time.sleep(WEBSOCKET_PING_INTERVAL)

    def monitor_timestamp(self, ws: WebSocketApp):
        """Monitor for new 15-minute timestamp and close connection when it changes."""
        # Calculate when the next 15-minute window starts (current timestamp + 15 minutes)
        next_timestamp = self.current_timestamp + (15 * 60)  # Add 15 minutes in seconds
        sleep_duration = next_timestamp - time.time()
        
        if sleep_duration > 0:
            print(f"Next 15-minute window starts in {sleep_duration:.0f} seconds")
            time.sleep(sleep_duration)
        
        print(f"New 15-minute window reached. Closing current connection to reconnect with new market data...")
        ws.close()

    def run(self):
        self.ws.run_forever()


def start_market_websocket_connection(crypto_name: str):
    """
    Start a WebSocket connection that automatically reconnects when a new 15-minute timestamp is reached.
    
    Args:
        crypto_name: The cryptocurrency name ('btc', 'eth', 'sol', 'xrp')
    """
    while True:
        try:
            print(f"\n{'='*60}")
            print(f"Fetching current 15-minute event tokens for {crypto_name.upper()}...")
            club_tokens = get_club_token_ids_from_15m_events()
            
            if crypto_name not in club_tokens:
                print(f"Error: No tokens found for {crypto_name}")
                time.sleep(60)
                continue
            
            token_ids = [club_tokens[crypto_name][0]]
            print(f"Found {len(token_ids)} token IDs for {crypto_name.upper()}")
            print(f"{'='*60}\n")
            
            market_connection = WebSocketOrderBook(
                MARKET_CHANNEL, WEBSOCKET_URL, token_ids, None, True, crypto_name
            )
            market_connection.run()
            
        except KeyboardInterrupt:
            print("\nShutting down...")
            break
        except Exception as e:
            print(f"Error occurred: {e}")
            print("Waiting 10 seconds before retrying...")
            time.sleep(10)


if __name__ == "__main__":
    start_market_websocket_connection('btc')
