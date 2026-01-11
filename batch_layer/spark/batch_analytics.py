#!/usr/bin/env python3
"""
Batch Analytics Job: Crypto Prediction Markets vs Spot Prices
Author: Batch Layer Team
Purpose: Analyze correlation between Polymarket predictions and Binance price movements
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_unixtime, unix_timestamp, window, avg, max, min, count,
    when, lit, concat_ws, expr, to_timestamp, first, last
)
from pyspark.sql.types import DoubleType, LongType
import sys
import config


def create_spark_session():
    """Initialize Spark session with Hive support"""
    return SparkSession.builder \
        .appName(config.APP_NAME) \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://localhost:9083") \
        .enableHiveSupport() \
        .getOrCreate()


def extract_crypto_from_market(market_str):
    """Extract crypto symbol from market field (btc/eth/sol/xrp)"""
    # Market field might be like "btc" or contain more info
    # We'll handle it with SQL CASE statement for safety
    return market_str


def load_binance_data(spark, date_filter=None):
    """
    Load Binance kline data from Hive
    
    Args:
        spark: SparkSession
        date_filter: Optional date string 'YYYY-MM-DD' to filter specific day
    
    Returns:
        DataFrame with processed Binance data
    """
    df = spark.sql(f"SELECT * FROM {config.BINANCE_TABLE}")
    
    if date_filter:
        df = df.filter(col("date") == date_filter)
    
    # Convert timestamp from milliseconds to timestamp type
    df = df.withColumn("ts", to_timestamp(col("timestamp") / 1000))
    
    # Map symbol to crypto name for joining
    df = df.withColumn("crypto", 
        when(col("symbol") == "BTCUSDT", "btc")
        .when(col("symbol") == "ETHUSDT", "eth")
        .when(col("symbol") == "SOLUSDT", "sol")
        .when(col("symbol") == "XRPUSDT", "xrp")
        .otherwise("unknown")
    )
    
    return df.select(
        col("ts").alias("binance_time"),
        "crypto",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "date"
    )


def load_polymarket_data(spark, date_filter=None):
    """
    Load Polymarket orderbook data from Hive
    
    Args:
        spark: SparkSession
        date_filter: Optional date string 'YYYY-MM-DD' to filter specific day
    
    Returns:
        DataFrame with processed Polymarket data
    """
    df = spark.sql(f"SELECT * FROM {config.POLYMARKET_TABLE}")
    
    if date_filter:
        df = df.filter(col("date") == date_filter)
    
    # Handle timestamp - it might be in various formats
    # Try to parse as Unix timestamp (milliseconds) or ISO format
    df = df.withColumn("ts",
        when(col("timestamp").cast("bigint").isNotNull(),
             to_timestamp(col("timestamp").cast("bigint") / 1000))
        .otherwise(to_timestamp(col("timestamp")))
    )
    
    # Cast last_trade_price from string to double
    df = df.withColumn("price", col("last_trade_price").cast(DoubleType()))
    
    # Clean market field - extract just the crypto symbol
    df = df.withColumn("crypto",
        when(col("market").contains("btc"), "btc")
        .when(col("market").contains("eth"), "eth")
        .when(col("market").contains("sol"), "sol")
        .when(col("market").contains("xrp"), "xrp")
        .otherwise(col("market"))
    )
    
    return df.select(
        col("ts").alias("poly_time"),
        "crypto",
        "asset_id",
        "price",
        "event_type",
        "date"
    ).filter(col("price").isNotNull())


def aggregate_to_windows(binance_df, polymarket_df):
    """
    Aggregate both datasets to 15-minute windows
    
    Args:
        binance_df: Binance DataFrame
        polymarket_df: Polymarket DataFrame
    
    Returns:
        Tuple of (binance_windows, polymarket_windows)
    """
    
    # Binance 15-minute aggregation (OHLC)
    binance_windows = binance_df.groupBy(
        window("binance_time", config.WINDOW_DURATION),
        "crypto",
        "symbol"
    ).agg(
        first("open").alias("window_open"),
        max("high").alias("window_high"),
        min("low").alias("window_low"),
        last("close").alias("window_close"),
        avg("volume").alias("avg_volume"),
        count("*").alias("candle_count")
    ).withColumn("window_start", col("window.start")) \
     .withColumn("window_end", col("window.end")) \
     .drop("window")
    
    # Polymarket 15-minute aggregation (probability metrics)
    polymarket_windows = polymarket_df.groupBy(
        window("poly_time", config.WINDOW_DURATION),
        "crypto"
    ).agg(
        avg("price").alias("avg_probability"),
        max("price").alias("max_probability"),
        min("price").alias("min_probability"),
        last("price").alias("last_probability"),
        count("*").alias("bet_activity_count")
    ).withColumn("window_start", col("window.start")) \
     .withColumn("window_end", col("window.end")) \
     .drop("window")
    
    return binance_windows, polymarket_windows


def join_and_analyze(binance_windows, polymarket_windows):
    """
    Join datasets and compute analytical metrics
    
    Args:
        binance_windows: Aggregated Binance data
        polymarket_windows: Aggregated Polymarket data
    
    Returns:
        DataFrame with analysis results
    """
    
    # Join on window start time and crypto symbol
    joined = binance_windows.join(
        polymarket_windows,
        (binance_windows.window_start == polymarket_windows.window_start) &
        (binance_windows.crypto == polymarket_windows.crypto),
        "inner"
    )
    
    # Calculate price movement
    joined = joined.withColumn(
        "price_movement",
        (col("window_close") - col("window_open")) / col("window_open")
    )
    
    # Determine market direction
    joined = joined.withColumn(
        "actual_direction",
        when(col("price_movement") > config.MIN_PRICE_MOVEMENT, "UP")
        .when(col("price_movement") < -config.MIN_PRICE_MOVEMENT, "DOWN")
        .otherwise("FLAT")
    )
    
    # Determine predicted direction based on probability
    joined = joined.withColumn(
        "predicted_direction",
        when(col("avg_probability") > config.PREDICTION_THRESHOLD, "UP")
        .when(col("avg_probability") < (1 - config.PREDICTION_THRESHOLD), "DOWN")
        .otherwise("UNCERTAIN")
    )
    
    # Calculate prediction accuracy
    joined = joined.withColumn(
        "prediction_result",
        when(
            (col("predicted_direction") == "UP") & (col("actual_direction") == "UP"),
            "CORRECT_BULL"
        ).when(
            (col("predicted_direction") == "DOWN") & (col("actual_direction") == "DOWN"),
            "CORRECT_BEAR"
        ).when(
            (col("predicted_direction") == "UP") & (col("actual_direction") == "DOWN"),
            "FAILED_BULL"
        ).when(
            (col("predicted_direction") == "DOWN") & (col("actual_direction") == "UP"),
            "FAILED_BEAR"
        ).otherwise("UNCERTAIN")
    )
    
    return joined.select(
        col("binance_windows.window_start").alias("window_start"),
        col("binance_windows.crypto").alias("crypto"),
        col("binance_windows.symbol").alias("symbol"),
        "window_open",
        "window_high",
        "window_low",
        "window_close",
        "avg_volume",
        "avg_probability",
        "max_probability",
        "bet_activity_count",
        "price_movement",
        "actual_direction",
        "predicted_direction",
        "prediction_result"
    )


def prepare_hbase_output(analysis_df):
    """
    Prepare data for HBase insertion
    
    Args:
        analysis_df: Analysis results DataFrame
    
    Returns:
        DataFrame formatted for HBase with row keys
    """
    
    # Create HBase row key: SYMBOL_TIMESTAMP
    # Using reverse timestamp for efficient range scans (newest first)
    df = analysis_df.withColumn(
        "rowkey",
        concat_ws("_",
            col("symbol"),
            (lit(9999999999999) - unix_timestamp(col("window_start")) * 1000).cast("string")
        )
    )
    
    # Prepare columns for HBase (flatten structure)
    df = df.select(
        col("rowkey"),
        col("window_start").cast("string").alias("timestamp"),
        col("crypto"),
        col("symbol"),
        col("window_open").alias("price_open"),
        col("window_close").alias("price_close"),
        col("window_high").alias("price_high"),
        col("window_low").alias("price_low"),
        col("avg_volume"),
        col("avg_probability"),
        col("max_probability"),
        col("bet_activity_count").cast("int"),
        col("price_movement"),
        col("actual_direction"),
        col("predicted_direction"),
        col("prediction_result")
    )
    
    return df


def write_to_hbase(df):
    """
    Write results to HBase
    
    Note: This uses foreachPartition with happybase.
    Make sure happybase is installed: pip install happybase
    
    Args:
        df: DataFrame to write
    """
    
    def write_partition(partition):
        """Write a partition to HBase"""
        try:
            import happybase
            connection = happybase.Connection(
                host=config.HBASE_HOST,
                port=config.HBASE_PORT,
                timeout=30000
            )
            table = connection.table(config.HBASE_TABLE_NAME)
            
            batch = table.batch()
            count = 0
            
            for row in partition:
                rowkey = row['rowkey'].encode('utf-8')
                
                # Build column dictionary
                data = {
                    # Price data column family
                    b'price_data:open': str(row['price_open']).encode('utf-8'),
                    b'price_data:close': str(row['price_close']).encode('utf-8'),
                    b'price_data:high': str(row['price_high']).encode('utf-8'),
                    b'price_data:low': str(row['price_low']).encode('utf-8'),
                    b'price_data:volume': str(row['avg_volume']).encode('utf-8'),
                    
                    # Betting data column family
                    b'bet_data:avg_prob': str(row['avg_probability']).encode('utf-8'),
                    b'bet_data:max_prob': str(row['max_probability']).encode('utf-8'),
                    b'bet_data:activity': str(row['bet_activity_count']).encode('utf-8'),
                    
                    # Analysis column family
                    b'analysis:price_movement': str(row['price_movement']).encode('utf-8'),
                    b'analysis:actual_direction': row['actual_direction'].encode('utf-8'),
                    b'analysis:predicted_direction': row['predicted_direction'].encode('utf-8'),
                    b'analysis:result': row['prediction_result'].encode('utf-8'),
                    b'analysis:timestamp': row['timestamp'].encode('utf-8'),
                    b'analysis:symbol': row['symbol'].encode('utf-8'),
                    b'analysis:crypto': row['crypto'].encode('utf-8')
                }
                
                batch.put(rowkey, data)
                count += 1
                
                # Commit in batches of 100
                if count % 100 == 0:
                    batch.send()
                    batch = table.batch()
            
            # Final commit
            batch.send()
            connection.close()
            print(f"Wrote {count} records to HBase")
            
        except Exception as e:
            print(f"Error writing to HBase: {e}")
            import traceback
            traceback.print_exc()
    
    # Convert to RDD and write
    df.foreachPartition(write_partition)


def save_as_parquet_backup(df, output_path):
    """
    Save results as Parquet for backup/debugging
    
    Args:
        df: DataFrame to save
        output_path: HDFS path
    """
    df.write.mode("overwrite").parquet(output_path)
    print(f"Backup saved to: {output_path}")


def main(date_filter=None):
    """
    Main execution function
    
    Args:
        date_filter: Optional date string 'YYYY-MM-DD' to process specific day
    """
    
    print("="*60)
    print("Crypto Market Batch Analytics - Starting")
    print("="*60)
    
    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Step 1: Load data
        print("\n[1/6] Loading Binance data from Hive...")
        binance_df = load_binance_data(spark, date_filter)
        binance_count = binance_df.count()
        print(f"      Loaded {binance_count} Binance records")
        
        print("\n[2/6] Loading Polymarket data from Hive...")
        polymarket_df = load_polymarket_data(spark, date_filter)
        poly_count = polymarket_df.count()
        print(f"      Loaded {poly_count} Polymarket records")
        
        if binance_count == 0 or poly_count == 0:
            print("\n[ERROR] No data found! Make sure:")
            print("  1. Tables are created: hive -f batch_layer/hive/create_tables.sql")
            print("  2. Partitions are repaired: hive -f batch_layer/hive/repair_partitions.sql")
            print("  3. Data exists in HDFS: hdfs dfs -ls /user/vagrant/cleansed/")
            return
        
        # Step 2: Aggregate to windows
        print("\n[3/6] Aggregating to 15-minute windows...")
        binance_windows, polymarket_windows = aggregate_to_windows(binance_df, polymarket_df)
        
        binance_windows.cache()
        polymarket_windows.cache()
        
        print(f"      Binance windows: {binance_windows.count()}")
        print(f"      Polymarket windows: {polymarket_windows.count()}")
        
        # Step 3: Join and analyze
        print("\n[4/6] Joining data and computing analytics...")
        analysis_df = join_and_analyze(binance_windows, polymarket_windows)
        analysis_df.cache()
        
        result_count = analysis_df.count()
        print(f"      Generated {result_count} analysis records")
        
        if result_count == 0:
            print("\n[WARNING] No matching windows found between Binance and Polymarket!")
            print("  This might mean the timestamps don't overlap.")
            return
        
        # Show sample results
        print("\n[SAMPLE] First 5 analysis results:")
        analysis_df.select(
            "window_start", "symbol", "window_open", "window_close",
            "price_movement", "avg_probability", "prediction_result"
        ).show(5, truncate=False)
        
        # Step 4: Prepare for HBase
        print("\n[5/6] Preparing HBase output format...")
        hbase_df = prepare_hbase_output(analysis_df)
        
        # Step 5: Write to HBase
        print("\n[6/6] Writing to HBase...")
        try:
            write_to_hbase(hbase_df)
            print("      Successfully wrote to HBase!")
        except Exception as e:
            print(f"      HBase write failed: {e}")
            print("      Saving to Parquet backup instead...")
            backup_path = "/user/vagrant/batch/analytics_results"
            save_as_parquet_backup(hbase_df, backup_path)
        
        # Summary statistics
        print("\n" + "="*60)
        print("ANALYSIS SUMMARY")
        print("="*60)
        analysis_df.groupBy("prediction_result").count().show()
        
        print("\nBy Symbol:")
        analysis_df.groupBy("symbol", "prediction_result").count().show()
        
    except Exception as e:
        print(f"\n[ERROR] Job failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
    
    print("\n" + "="*60)
    print("Batch Analytics - Completed")
    print("="*60)


if __name__ == "__main__":
    # Allow date filter from command line
    # Usage: spark-submit batch_analytics.py [YYYY-MM-DD]
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    main(date_filter=date_arg)
