import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger
from config import settings
import io
import time
from hdfs import InsecureClient

class StorageManager:
    def __init__(self):
        self.hdfs_root = "/user/vagrant/raw/binance"
        
        try:
            self.client = InsecureClient('http://localhost:50070', user='vagrant')
            # Test connection briefly
            self.client.status('/')
        except Exception as e:
            logger.critical(f"Failed to connect to HDFS at localhost:50070. Error: {e}")
            self.client = None

    def save_chunk(self, df: pd.DataFrame, symbol: str):
        """
        Saves a dataframe chunk to HDFS as a Parquet file.
        Manually partitions by symbol and date.
        """
        if df.empty:
            return
        
        if not self.client:
            logger.error("HDFS client is not active. Data will NOT be saved.")
            return

        try:
            # 1. Ensure timestamp is datetime
            if 'timestamp' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')

            # 2. Add date column for partitioning
            df['date'] = df['timestamp'].dt.date.astype(str)

            # 3. Add symbol column if missing
            if 'symbol' not in df.columns:
                df['symbol'] = symbol

            # 4. Iterate over unique dates in this chunk to partition correctly
            unique_dates = df['date'].unique()

            for date_str in unique_dates:
                # Filter data for this specific partition
                partition_df = df[df['date'] == date_str].copy()
                
                # Convert to PyArrow Table
                table = pa.Table.from_pandas(partition_df)
                
                # Write Parquet file to an in-memory buffer
                buffer = io.BytesIO()
                pq.write_table(table, buffer)
                buffer.seek(0) # Reset buffer pointer
                
                # Construct HDFS Directory Path
                # Structure: .../raw/binance/symbol=BTCUSDT/date=2025-01-05/
                hdfs_dir = f"{self.hdfs_root}/symbol={symbol}/date={date_str}"
                
                # Construct Unique Filename (timestamp + row count to ensure uniqueness)
                filename = f"{int(time.time() * 1000)}_{len(partition_df)}.parquet"
                full_path = f"{hdfs_dir}/{filename}"

                # Ensure directory exists (WebHDFS often needs explicit creation)
                try:
                    self.client.makedirs(hdfs_dir)
                except Exception:
                    pass # Ignore if exists

                # Write buffer to HDFS
                with self.client.write(full_path, overwrite=False) as writer:
                    writer.write(buffer.getvalue())

                logger.info(f"Saved {len(partition_df)} records for {symbol} to HDFS: {full_path}")

        except Exception as e:
            logger.error(f"Failed to save chunk for {symbol}: {e}")

storage = StorageManager()
