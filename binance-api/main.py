import asyncio
import sys
from loguru import logger
from src.historical import historical_fetcher
from src.stream import stream_ingestor
from config import settings

# Configure logger
logger.remove()
logger.add(sys.stderr, level="INFO")
logger.add("app.log", rotation="10 MB")

async def main():
    logger.info("Starting Binance Data Ingestor...")
    
    # 1. Backfill Historical Data
    logger.info("Starting historical backfill...")
    for symbol in settings.SYMBOLS:
        await historical_fetcher.fetch_historical_data(symbol, start_str="1 day ago UTC")
    
    await historical_fetcher.close()
    logger.info("Historical backfill complete.")
    
    # 2. Start Real-time Stream
    logger.info("Starting real-time stream...")
    try:
        # Create a task for the stream
        stream_task = asyncio.create_task(stream_ingestor.start_stream())
        
        # Wait for the task, or interrupt
        await stream_task
    except KeyboardInterrupt:
        logger.info("Stopping stream...")
        stream_ingestor.stop()
        await stream_task # Wait for cleanup
    except Exception as e:
        logger.critical(f"Stream failed: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
