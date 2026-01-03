from pydantic_settings import BaseSettings
from pathlib import Path

class Settings(BaseSettings):
    # Binance API (Optional for public data, but good to have)
    BINANCE_API_KEY: str = ""
    BINANCE_API_SECRET: str = ""

    # Data Configuration
    SYMBOLS: list[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
    HISTORICAL_INTERVAL: str = "1m"
    STREAM_INTERVAL: str = "1m"
    
    # Storage
    BASE_DIR: Path = Path(__file__).parent
    DATA_DIR: Path = BASE_DIR / "data"
    
    # Streaming
    BUFFER_SIZE: int = 100  # Number of records to buffer before writing (increase for better performance)
    SNAPSHOT_INTERVAL_SECONDS: int = 1  # Interval for sampling real-time data snapshots
    
    class Config:
        env_file = ".env"

settings = Settings()

# Ensure data directory exists
settings.DATA_DIR.mkdir(exist_ok=True)
