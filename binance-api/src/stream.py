import asyncio
import pandas as pd
from binance import AsyncClient, BinanceSocketManager
from loguru import logger
from config import settings
from src.storage import storage
import time

class StreamIngestor:
    def __init__(self):
        self.buffer = []
        self.buffer_limit = settings.BUFFER_SIZE
        self.stop_event = asyncio.Event()
        self.last_snapshot_time = {} # symbol -> last_time

    async def start_stream(self):
        """
        Connects to Binance WebSocket and streams data for configured symbols.
        """
        client = await AsyncClient.create(settings.BINANCE_API_KEY, settings.BINANCE_API_SECRET)
        bm = BinanceSocketManager(client)
        
        # streams format: <symbol>@kline_<interval>
        streams = [f"{s.lower()}@kline_{settings.STREAM_INTERVAL}" for s in settings.SYMBOLS]
        
        logger.info(f"Starting stream for: {streams}")
        
        retry_count = 0
        max_attempts = 3
        
        while retry_count < max_attempts and not self.stop_event.is_set():
            try:
                ts = bm.multiplex_socket(streams)
                async with ts as tscm:
                    retry_count = 0 # Reset retry count on successful connection
                    logger.info("WebSocket connected.")
                    
                    while not self.stop_event.is_set():
                        try:
                            # Wait for message with timeout to allow checking stop_event
                            res = await asyncio.wait_for(tscm.recv(), timeout=1.0)
                            if res:
                                await self.process_message(res)
                        except asyncio.TimeoutError:
                            continue
                        except Exception as e:
                            logger.error(f"WebSocket error: {e}")
                            raise e # Trigger reconnection
                            
            except Exception as e:
                logger.error(f"Connection failed (attempt {retry_count + 1}/{max_attempts}): {e}")
                retry_count += 1
                if retry_count < max_attempts:
                    await asyncio.sleep(2) # Wait before retrying
                else:
                    logger.critical("Max attempts reached. Exiting stream.")
                    raise e

        await client.close_connection()
        logger.info("Stream stopped.")

    async def process_message(self, msg):
        """
        Processes a single WebSocket message.
        Samples data every 1 second.
        """
        if 'data' not in msg:
            return

        data = msg['data']
        kline = data['k']
        symbol = data['s']
        
        current_time = time.time()
        last_time = self.last_snapshot_time.get(symbol, 0)
        
        # Sample at configured interval OR if candle closed
        if kline['x'] or (current_time - last_time >= settings.SNAPSHOT_INTERVAL_SECONDS):
            record = {
                'timestamp': kline['t'], # Open time
                'snapshot_time': int(current_time * 1000), # Capture time
                'open': float(kline['o']),
                'high': float(kline['h']),
                'low': float(kline['l']),
                'close': float(kline['c']),
                'volume': float(kline['v']),
                'close_time': kline['T'],
                'quote_asset_volume': float(kline['q']),
                'trades': int(kline['n']),
                'taker_buy_base': float(kline['V']),
                'taker_buy_quote': float(kline['Q']),
                'symbol': symbol,
                'is_closed': kline['x']
            }
            
            self.buffer.append(record)
            self.last_snapshot_time[symbol] = current_time
            
            if len(self.buffer) >= self.buffer_limit:
                self.flush_buffer()

    def flush_buffer(self):
        if not self.buffer:
            return
            
        df = pd.DataFrame(self.buffer)
        
        for symbol, group in df.groupby('symbol'):
            storage.save_chunk(group.copy(), symbol)
            
        self.buffer = []
        logger.info("Flushed buffer to storage.")
        
    def stop(self):
        self.stop_event.set()

stream_ingestor = StreamIngestor()
