from pydantic_settings import BaseSettings
from pathlib import Path

class Settings(BaseSettings):
    # Binance API (Optional for public data, but good to have)
    BINANCE_API_KEY: str = ""
    BINANCE_API_SECRET: str = ""

    # Data Configuration
    SYMBOLS: list[str] = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
    HISTORICAL_INTERVAL: str = "1m"
    STREAM_INTERVAL: str = "1s"
    
    # Storage
    BASE_DIR: Path = Path(__file__).parent
    DATA_DIR: Path = BASE_DIR / "data"
    
    # Streaming
    BUFFER_SIZE: int = 4  # Number of records to buffer before writing
    
    class Config:
        env_file = ".env"

settings = Settings()

# Ensure data directory exists
settings.DATA_DIR.mkdir(exist_ok=True)
