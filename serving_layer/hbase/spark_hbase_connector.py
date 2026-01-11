"""
Spark-HBase Connector Module for Crypto Options vs Rates Project

Provides PySpark integration for writing DataFrames to HBase tables.
Used by both Batch Layer (Spark jobs) and Speed Layer (Structured Streaming).
"""

import sys
from typing import Dict, Optional, Callable
from functools import partial

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import col, udf
    from pyspark.sql.types import StringType
except ImportError:
    print("PySpark not available. This module requires Spark context.")
    DataFrame = None

try:
    import happybase
except ImportError:
    print("HappyBase not installed. Install with: pip install happybase")
    happybase = None

# Import configuration
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (
    HBASE_HOST,
    HBASE_PORT,
    TABLE_MARKET_ANALYTICS,
    TABLE_MARKET_LIVE,
    CF_PRICES,
    CF_BETTING,
    CF_CORRELATION,
    CF_DATA,
    generate_batch_rowkey,
    generate_live_rowkey,
)


# ==============================================================================
# Batch Layer: Write Spark DataFrame to HBase
# ==============================================================================

def write_batch_to_hbase(
    df: DataFrame,
    table_name: str = TABLE_MARKET_ANALYTICS,
    host: str = HBASE_HOST,
    port: int = HBASE_PORT,
    batch_size: int = 1000
):
    """
    Write a Spark DataFrame to HBase market_analytics table.
    
    Expected DataFrame columns (aligned with batch_layer/spark/batch_analytics.py):
        - symbol: String (e.g., 'BTC')
        - window_end_ts: Long (Unix timestamp)
        - price_open, price_close, price_high, price_low, avg_volume: Double (price_data)
        - avg_probability, max_probability, bet_activity_count: Double (bet_data)
        - price_movement, actual_direction, predicted_direction, prediction_result: String (analysis)
    
    Args:
        df: Spark DataFrame with the expected schema
        table_name: HBase table name (default: market_analytics)
        host: HBase Thrift host
        port: HBase Thrift port
        batch_size: Number of rows per HBase batch write
    """
    def write_partition(partition, host, port, table_name, batch_size):
        """Write a partition to HBase using HappyBase."""
        import happybase
        
        connection = happybase.Connection(host, port)
        connection.open()
        table = connection.table(table_name)
        
        batch = table.batch(batch_size=batch_size)
        count = 0
        
        for row in partition:
            # Generate RowKey
            rowkey = f"{row['symbol']}#{row['window_end_ts']}"
            
            # Build data dict
            data = {}
            
            # price_data column family
            for col_name in ['open', 'close', 'high', 'low', 'volume']:
                if row.get(col_name) is not None:
                    data[f'price_data:{col_name}'.encode()] = str(row[col_name]).encode()
            for src, dest in [('price_open', 'open'), ('price_close', 'close'), 
                             ('price_high', 'high'), ('price_low', 'low'), ('avg_volume', 'volume')]:
                if row.get(src) is not None:
                    data[f'price_data:{dest}'.encode()] = str(row[src]).encode()
            
            # bet_data column family
            for col_name in ['avg_prob', 'max_prob', 'activity']:
                if row.get(col_name) is not None:
                    data[f'bet_data:{col_name}'.encode()] = str(row[col_name]).encode()
            for src, dest in [('avg_probability', 'avg_prob'), ('max_probability', 'max_prob'),
                             ('bet_activity_count', 'activity')]:
                if row.get(src) is not None:
                    data[f'bet_data:{dest}'.encode()] = str(row[src]).encode()
            
            # analysis column family
            for col_name in ['result', 'price_movement', 'actual_direction', 'predicted_direction',
                            'timestamp', 'symbol', 'crypto']:
                if row.get(col_name) is not None:
                    data[f'analysis:{col_name}'.encode()] = str(row[col_name]).encode()
            if row.get('prediction_result') is not None:
                data[b'analysis:result'] = str(row['prediction_result']).encode()
            
            batch.put(rowkey.encode(), data)
            count += 1
        
        batch.send()
        connection.close()
        return count
    
    # Convert DataFrame to RDD and process partitions
    df.rdd.foreachPartition(
        lambda partition: write_partition(partition, host, port, table_name, batch_size)
    )


# ==============================================================================
# Speed Layer: Write Streaming DataFrame to HBase
# ==============================================================================

def write_stream_to_hbase(
    df: DataFrame,
    table_name: str = TABLE_MARKET_LIVE,
    host: str = HBASE_HOST,
    port: int = HBASE_PORT,
    checkpoint_location: str = "/tmp/spark-hbase-checkpoint"
):
    """
    Write a Streaming DataFrame to HBase market_live table.
    
    Expected DataFrame columns:
        - symbol: String
        - timestamp: Long (milliseconds)
        - binance_price: Double
        - poly_last_trade: Double (optional)
        - poly_best_bid: Double (optional)
        - poly_best_ask: Double (optional)
        - implied_prob: Double (optional)
    
    Args:
        df: Spark Streaming DataFrame
        table_name: HBase table name (default: market_live)
        host: HBase Thrift host
        port: HBase Thrift port
        checkpoint_location: Checkpoint directory for streaming
        
    Returns:
        StreamingQuery object
    """
    def write_micro_batch(batch_df, batch_id, host, port, table_name):
        """Process each micro-batch and write to HBase."""
        if batch_df.isEmpty():
            return
        
        def write_partition(partition):
            import happybase
            from config import REVERSE_TIMESTAMP_MAX
            
            connection = happybase.Connection(host, port)
            connection.open()
            table = connection.table(table_name)
            
            for row in partition:
                # Generate reverse timestamp RowKey
                reverse_ts = REVERSE_TIMESTAMP_MAX - row['timestamp']
                rowkey = f"{row['symbol']}#{reverse_ts}"
                
                # Build data dict
                data = {
                    b'd:binance_price': str(row['binance_price']).encode()
                }
                
                for col_name in ['poly_last_trade', 'poly_best_bid', 'poly_best_ask', 'implied_prob']:
                    if row.get(col_name) is not None:
                        data[f'd:{col_name}'.encode()] = str(row[col_name]).encode()
                
                table.put(rowkey.encode(), data)
            
            connection.close()
        
        batch_df.rdd.foreachPartition(write_partition)
    
    # Start streaming query with foreachBatch
    query = (
        df.writeStream
        .foreachBatch(lambda batch_df, batch_id: write_micro_batch(batch_df, batch_id, host, port, table_name))
        .option("checkpointLocation", checkpoint_location)
        .start()
    )
    
    return query


# ==============================================================================
# DataFrame Builder Utilities
# ==============================================================================

def create_batch_analytics_df(spark: 'SparkSession', data: list) -> DataFrame:
    """
    Create a DataFrame suitable for batch layer HBase writes.
    
    Args:
        spark: SparkSession
        data: List of dicts with batch analytics data
        
    Returns:
        Spark DataFrame
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, LongType, DoubleType
    )
    
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("window_end_ts", LongType(), False),
        # Prices
        StructField("open", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("volatility", DoubleType(), True),
        # Betting
        StructField("avg_prob_up", DoubleType(), True),
        StructField("avg_prob_down", DoubleType(), True),
        StructField("total_bet_vol", DoubleType(), True),
        StructField("sentiment_score", DoubleType(), True),
        # Correlation
        StructField("prediction_result", StringType(), True),
        StructField("divergence", DoubleType(), True),
    ])
    
    return spark.createDataFrame(data, schema)


def create_live_data_df(spark: 'SparkSession', data: list) -> DataFrame:
    """
    Create a DataFrame suitable for speed layer HBase writes.
    
    Args:
        spark: SparkSession
        data: List of dicts with live data
        
    Returns:
        Spark DataFrame
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, LongType, DoubleType
    )
    
    schema = StructType([
        StructField("symbol", StringType(), False),
        StructField("timestamp", LongType(), False),
        StructField("binance_price", DoubleType(), False),
        StructField("poly_last_trade", DoubleType(), True),
        StructField("poly_best_bid", DoubleType(), True),
        StructField("poly_best_ask", DoubleType(), True),
        StructField("implied_prob", DoubleType(), True),
    ])
    
    return spark.createDataFrame(data, schema)


# ==============================================================================
# Usage Example
# ==============================================================================

def example_batch_write():
    """Example showing batch DataFrame write to HBase."""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("HBaseBatchExample") \
        .getOrCreate()
    
    # Sample data
    data = [
        {
            "symbol": "BTC",
            "window_end_ts": 1767201300,
            "open": 92000.0,
            "close": 92500.5,
            "high": 92800.0,
            "low": 91900.0,
            "volume": 150000.0,
            "volatility": 0.02,
            "avg_prob_up": 0.65,
            "avg_prob_down": 0.35,
            "total_bet_vol": 50000.0,
            "sentiment_score": 0.15,
            "prediction_result": "CORRECT",
            "divergence": 0.05
        }
    ]
    
    df = create_batch_analytics_df(spark, data)
    write_batch_to_hbase(df)
    
    print("Batch write complete!")
    spark.stop()


if __name__ == '__main__':
    print("Spark-HBase Connector Module")
    print("=" * 50)
    print("\nUsage in Spark job:")
    print("  from spark_hbase_connector import write_batch_to_hbase, create_batch_analytics_df")
    print("  df = create_batch_analytics_df(spark, data)")
    print("  write_batch_to_hbase(df)")
    print("\nFor streaming:")
    print("  from spark_hbase_connector import write_stream_to_hbase")
    print("  query = write_stream_to_hbase(streaming_df)")
    print("  query.awaitTermination()")
