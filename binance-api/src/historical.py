import asyncio
import pandas as pd
from binance import AsyncClient
from loguru import logger
from config import settings
from src.storage import storage

class HistoricalFetcher:
    def __init__(self):
        self.client = None

    async def _get_client(self):
        if not self.client:
            self.client = await AsyncClient.create(
                settings.BINANCE_API_KEY, 
                settings.BINANCE_API_SECRET
            )
        return self.client

    async def fetch_historical_data(self, symbol: str, start_str: str = "1 day ago UTC"):
        """
        Fetches historical klines for a symbol and saves them.
        """
        client = await self._get_client()
        logger.info(f"Starting historical fetch for {symbol} from {start_str}...")
        
        try:
            klines = await client.get_historical_klines(
                symbol, 
                settings.HISTORICAL_INTERVAL, 
                start_str
            )
            
            if not klines:
                logger.warning(f"No data found for {symbol}")
                return

            # Binance kline format:
            # [
            #   [
            #     1499040000000,      // Open time
            #     "0.01634790",       // Open
            #     "0.80000000",       // High
            #     "0.01575800",       // Low
            #     "0.01577100",       // Close
            #     "148976.11427815",  // Volume
            #     1499644799999,      // Close time
            #     "2434.19055334",    // Quote asset volume
            #     308,                // Number of trades
            #     "1756.87402397",    // Taker buy base asset volume
            #     "28.46694368",      // Taker buy quote asset volume
            #     "17928899.62484339" // Ignore.
            #   ]
            # ]
            
            columns = [
                'timestamp', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_asset_volume', 'trades', 
                'taker_buy_base', 'taker_buy_quote', 'ignore'
            ]
            
            df = pd.DataFrame(klines, columns=columns)
            
            # Type conversion
            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'quote_asset_volume', 'taker_buy_base', 'taker_buy_quote']
            df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, axis=1)
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # Align schema with StreamIngestor
            df.drop(columns=['ignore'], inplace=True)
            df['is_closed'] = True
            # For historical closed candles, snapshot time can be close time
            df['snapshot_time'] = df['close_time']
            
            # Save to storage
            storage.save_chunk(df, symbol)
            logger.success(f"Successfully fetched and saved historical data for {symbol}")
            
        except Exception as e:
            logger.error(f"Error fetching historical data for {symbol}: {e}")
        
    async def close(self):
        if self.client:
            await self.client.close_connection()

historical_fetcher = HistoricalFetcher()
