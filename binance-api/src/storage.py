import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from loguru import logger
from config import settings

class StorageManager:
    def __init__(self):
        self.data_dir = settings.DATA_DIR

    def save_chunk(self, df: pd.DataFrame, symbol: str):
        """
        Saves a dataframe chunk to a partitioned parquet dataset.
        Partitioning strategy: symbol / date
        """
        if df.empty:
            return

        # Ensure timestamp is datetime
        if 'timestamp' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

        # Add date column for partitioning
        df['date'] = df['timestamp'].dt.date.astype(str)

        # Define table
        table = pa.Table.from_pandas(df)
        file_name = f"{datetime.now().timestamp()}.parquet"
        
        if 'symbol' not in df.columns:
            df['symbol'] = symbol
            
        table = pa.Table.from_pandas(df)
        
        try:
            pq.write_to_dataset(
                table,
                root_path=str(self.data_dir),
                partition_cols=['symbol', 'date'],
                existing_data_behavior='overwrite_or_ignore'
            )
            logger.info(f"Saved chunk for {symbol} with {len(df)} records.")
        except Exception as e:
            logger.error(f"Failed to save chunk for {symbol}: {e}")

storage = StorageManager()
