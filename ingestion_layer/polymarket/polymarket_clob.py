from websocket import WebSocketApp
import json
import time
import threading
import sys
import os
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List
from hdfs import InsecureClient

parent_dir = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(parent_dir))

from ingestion_layer.polymarket.polymarket_gamma import (
    get_club_token_ids_from_15m_events, 
    get_current_15m_events
)
from ingestion_layer.polymarket.configs.polymarket_config import (
    MARKET_CHANNEL,
    WEBSOCKET_URL,
    WEBSOCKET_PING_INTERVAL
)
from ingestion_layer.polymarket.utils.time_utils import current_quarter_timestamp_et


def save_metadata_to_hdfs(events_data, timestamp):
    """Saves the full Gamma API response to HDFS."""
    try:
        date_str = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')
        # STRICT PATH: /user/vagrant/raw/...
        hdfs_dir = f"/user/vagrant/raw/polymarket_metadata/date={date_str}"
        filename = f"events_meta_{timestamp}.json"
        
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir], stderr=subprocess.DEVNULL)

        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp:
            json.dump(events_data, tmp)
            tmp_path = tmp.name

        hdfs_dest = f"{hdfs_dir}/{filename}"
        print(f"   [METADATA] Saving context to: {hdfs_dest}")
        subprocess.run(["hdfs", "dfs", "-put", "-f", tmp_path, hdfs_dest], stderr=subprocess.DEVNULL)
        os.remove(tmp_path)
    except Exception as e:
        print(f"   [!] Error saving metadata: {e}")

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
        
        # Connects to HDFS (Standard port: 50070 for Hadoop 3.x, 50070 for Hadoop 2.x)
        self.hdfs_client = InsecureClient('http://localhost:50070', user='vagrant')
        self.hdfs_path = "/user/vagrant/raw/polymarket_trade/clob_stream.json"
        # Buffering to prevent HDFS overload
        self.buffer = []
        self.buffer_limit = 50
        self.last_flush_time = time.time()
        
        furl = url + "/ws/" + channel_type
        self.ws = WebSocketApp(
            furl,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open,
        )

    def flush_to_hdfs(self):
        """Writes the current buffer to HDFS."""
        if not self.buffer:
            return

        try:
            # Join messages with newlines
            data_chunk = "\n".join(self.buffer) + "\n"
            
            # Try appending to the main file
            try:
                with self.hdfs_client.write(self.hdfs_path, append=True, encoding='utf-8') as writer:
                    writer.write(data_chunk)
            except Exception:
                # Fallback: If append is not supported, write a new timestamped file
                timestamp = int(time.time() * 1000)
                fallback_path = f"{self.hdfs_path}_{timestamp}.json"
                with self.hdfs_client.write(fallback_path, encoding='utf-8') as writer:
                    writer.write(data_chunk)
            
            if self.verbose:
                print(f"[HDFS] Flushed {len(self.buffer)} records.")
                
            self.buffer = []
            self.last_flush_time = time.time()
            
        except Exception as e:
            print(f"[!] HDFS Write Error: {e}")

    def on_message(self, ws: WebSocketApp, message: str):
        # Skip "PONG" messages - they break the NiFi JSON parser
        if "PONG" in message:
            return
        
        # Skip empty messages ([], {}, whitespace-only)
        stripped = message.strip()
        if stripped in ('[]', '{}', ''):
            return
        
        # Skip messages that are just empty arrays/objects after parsing
        try:
            parsed = json.loads(message)
            if parsed in ([], {}, None):
                return
        except json.JSONDecodeError:
            # If it's not valid JSON, skip it
            return
        
        self.buffer.append(message)
        
        # Check if we should flush (Batch size reached OR Time elapsed)
        if len(self.buffer) >= self.buffer_limit or (time.time() - self.last_flush_time > 10):
            self.flush_to_hdfs()

    def on_error(self, ws: WebSocketApp, error: str):
        print("Error: ", error)
        self.flush_to_hdfs()
        exit(1)

    def on_close(self, ws: WebSocketApp, close_status_code: int, close_msg: str):
        print(f"Closing connection (status: {close_status_code}, msg: {close_msg})")
        self.flush_to_hdfs()
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
        self.flush_to_hdfs()
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
            
            try:
                events_meta = get_current_15m_events()
                save_metadata_to_hdfs(events_meta, current_quarter_timestamp_et())
            except Exception as e:
                print(f"[WARN] Could not save metadata: {e}")

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
