"""
Crypto Options vs Rates - Speed Layer Stream Processing

This module implements real-time stream processing for cryptocurrency price data
from Binance and prediction market data from Polymarket using Apache Spark Structured
Streaming. It consumes data from Kafka topics, performs transformations, and computes
windowed aggregations for near real-time analytics.

Key Features:
    - Ingests streaming data from multiple Kafka topics
    - Applies schema validation and data transformations
    - Calculates market sentiment and order book imbalance metrics
    - Performs windowed aggregations (1-minute tumbling windows)
    - Outputs results to console (can be modified for HBase/HDFS storage)

Data Sources:
    - binance: Real-time cryptocurrency prices, volumes, and trading activity
    - polymarket_trade: Prediction market trades and order book snapshots

Output Metrics:
    Binance:
        - Average price, volatility (stddev), total USDT volume
        - Buy sentiment ratio (taker buy volume / total volume)
    
    Polymarket:
        - Average probability (mid-price), order book imbalance
        - Average spread, total shares traded, number of trades

Author: Speed Layer Team
Last Modified: 2026-01-11
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, expr, array_max, array_min, 
    explode, window, avg, sum, count, stddev, abs, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, ArrayType
)

# --- 1. CONFIGURATION ---
# Kafka broker address for stream ingestion
# HDFS checkpoint location for fault-tolerant state management
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
CHECKPOINT_LOCATION = "hdfs:///tmp/checkpoints" 

# Initialize Spark Session with optimized configurations for streaming
# - stopGracefullyOnShutdown: Ensures proper cleanup on termination
# - shuffle.partitions: Reduced to 4 for low-latency processing
spark = SparkSession.builder \
    .appName("CryptoOptions_SpeedLayer") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Reduce log verbosity to focus on warnings and errors
spark.sparkContext.setLogLevel("WARN")

# --- 2. SCHEMAS ---
# Define structured schemas for incoming JSON data from Kafka topics
# These schemas ensure type safety and enable efficient columnar processing

# A. Binance Schema
# Represents cryptocurrency kline/candlestick data with volume metrics
# Fields:
#   - timestamp: Data collection timestamp (ms)
#   - snapshot_time: Market event time (ms)
#   - symbol: Trading pair (e.g., "SOLUSDT")
#   - close: Closing price for the interval
#   - volume: Base asset volume (e.g., SOL traded)
#   - quote_asset_volume: Quote asset volume (USDT value of trades)
#   - trades: Number of trades in the interval
#   - taker_buy_base: Volume of taker buy orders (base asset)
#   - taker_buy_quote: Volume of taker buy orders (quote asset/USDT)
binance_schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("snapshot_time", LongType(), True),     # Event Time (ms)
    StructField("symbol", StringType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),          # Base Asset Vol (SOL)
    StructField("quote_asset_volume", DoubleType(), True), # Quote Asset Vol (USDT)
    StructField("trades", LongType(), True),
    StructField("taker_buy_base", DoubleType(), True),
    StructField("taker_buy_quote", DoubleType(), True)  # Taker Buy Vol (USDT)
])

# B. Polymarket Schema
# Represents prediction market order book and trade data
# Order book entries contain price levels and corresponding sizes
order_item = StructType([
    StructField("price", StringType(), True),  # Price level (probability 0-1)
    StructField("size", StringType(), True)    # Liquidity at this price
])

# Main Polymarket message schema
# Fields:
#   - event_type: Type of event ("book", "trade", etc.)
#   - market: Unique market identifier
#   - timestamp: Event timestamp (ms)
#   - bids: Array of bid orders (buyers)
#   - asks: Array of ask orders (sellers)
#   - price: Trade execution price (for trade events)
#   - size: Trade execution size (for trade events)
poly_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("market", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("bids", ArrayType(order_item), True),
    StructField("asks", ArrayType(order_item), True),
    StructField("price", StringType(), True),
    StructField("size", StringType(), True)
])

# --- 3. READ STREAMS ---

def read_kafka(topic):
    """
    Create a streaming DataFrame from a Kafka topic.
    
    Configures a Spark Structured Streaming source to consume messages from
    the specified Kafka topic. Messages are read starting from the latest offset
    to process only new data (not historical backlog).
    
    Args:
        topic (str): Name of the Kafka topic to subscribe to
        
    Returns:
        DataFrame: Streaming DataFrame with Kafka message schema:
            - key: Message key (binary)
            - value: Message value (binary)
            - topic: Source topic name
            - partition: Kafka partition ID
            - offset: Message offset
            - timestamp: Kafka message timestamp
    """
    return spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

# Initialize streaming sources for both data streams
raw_binance = read_kafka("binance")
raw_poly = read_kafka("polymarket_trade")

# --- 4. TRANSFORMATIONS ---
# Parse JSON payloads and extract relevant fields with computed metrics

# === BINANCE STREAM ===
# Transform raw Kafka messages into structured market data with sentiment indicators
# Steps:
#   1. Cast binary value to JSON string
#   2. Parse JSON array using defined schema
#   3. Explode array to individual records
#   4. Extract and calculate key metrics
binance_clean = raw_binance.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), ArrayType(binance_schema)).alias("data")) \
    .select(explode(col("data")).alias("r")) \
    .select(
        col("r.symbol"),
        (col("r.snapshot_time") / 1000).cast("timestamp").alias("event_time"),
        col("r.close").alias("price"),
        col("r.quote_asset_volume").alias("usdt_volume"),  # Total USDT volume traded
        
        # Buy Sentiment Indicator:
        # Ratio of aggressive buyer volume to total volume (in USDT terms)
        # Interpretation:
        #   - 0.5: Neutral market (equal buying/selling pressure)
        #   - >0.5: Bullish sentiment (more aggressive buyers)
        #   - <0.5: Bearish sentiment (more aggressive sellers)
        # Returns null if quote_asset_volume is zero to avoid division by zero
        when(col("r.quote_asset_volume") > 0, 
             col("r.taker_buy_quote") / col("r.quote_asset_volume")).alias("buy_sentiment")
    )

# === POLYMARKET STREAM ===
# Transform raw prediction market data into structured metrics
# Calculates order book imbalance to measure buying/selling pressure
# Steps:
#   1. Parse JSON messages using Polymarket schema
#   2. Extract best bid/ask prices and sizes
#   3. Compute derived metrics (spread, mid-price, imbalance)
poly_clean = raw_poly.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), poly_schema).alias("data")) \
    .select(
        col("data.market").alias("market_id"),
        col("data.event_type"),
        (col("data.timestamp").cast("long") / 1000).cast("timestamp").alias("event_time"),
        
        # Order Book Price Levels:
        # best_bid: Highest price buyers are willing to pay (max probability)
        # best_ask: Lowest price sellers are willing to accept (min probability)
        array_max(expr("transform(data.bids, x -> cast(x.price as double))")).alias("best_bid"),
        array_min(expr("transform(data.asks, x -> cast(x.price as double))")).alias("best_ask"),
        
        # Order Book Depth (Liquidity):
        # Extract the size (quantity) available at the best price levels
        # Used to calculate order book imbalance (bid pressure vs ask pressure)
        # Uses element_at with safe access - returns null if filter result is empty
        expr("element_at(filter(data.bids, x -> x.price == array_max(transform(data.bids, y -> y.price))), 1).size").cast("double").alias("bid_depth"),
        expr("element_at(filter(data.asks, x -> x.price == array_min(transform(data.asks, y -> y.price))), 1).size").cast("double").alias("ask_depth"),
        
        # Trade Execution Data:
        col("data.size").cast("double").alias("trade_size"),
        col("data.price").cast("double").alias("trade_price")
    ) \
    .withColumn("spread", abs(col("best_ask") - col("best_bid"))) \
    .withColumn("mid_price_prob", (col("best_bid") + col("best_ask")) / 2) \
    .withColumn("book_imbalance", 
                # Order Book Imbalance Ratio:
                # Measures relative pressure from buyers vs sellers
                # Range: [-1, 1]
                #   +1: All liquidity on bid side (strong buying pressure)
                #   -1: All liquidity on ask side (strong selling pressure)
                #    0: Balanced order book
                # Returns null if total depth is zero to avoid division by zero
                when((col("bid_depth") + col("ask_depth")) > 0,
                     (col("bid_depth") - col("ask_depth")) / (col("bid_depth") + col("ask_depth")))
    )

# --- 5. WINDOWED AGGREGATIONS ---
# Compute rolling statistics over 1-minute tumbling windows
# Watermarking (2 minutes) handles late-arriving data gracefully

# BINANCE AGGREGATIONS
# Calculates price statistics, volume, and sentiment for each trading pair
binance_agg = binance_clean \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(window(col("event_time"), "1 minute"), col("symbol")) \
    .agg(
        avg("price").alias("avg_price"),                  # Mean price over window
        stddev("price").alias("volatility"),             # Price volatility (std dev)
        sum("usdt_volume").alias("total_usdt_volume"),   # Total USDT traded
        avg("buy_sentiment").alias("avg_sentiment"),     # Average buying pressure
        count("*").alias("ticks")                        # Number of data points
    )

# POLYMARKET AGGREGATIONS
# Calculates market probability, liquidity metrics, and trading activity
poly_agg = poly_clean \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(window(col("event_time"), "1 minute"), col("market_id")) \
    .agg(
        avg("mid_price_prob").alias("avg_prob"),          # Average market probability
        avg("book_imbalance").alias("avg_imbalance"),     # Average order book pressure
        avg("spread").alias("avg_spread"),                # Average bid-ask spread
        sum("trade_size").alias("total_shares_traded"),   # Total trading volume
        count("trade_price").alias("num_trades")          # Number of trades executed
    )

# --- 6. OUTPUT SINKS ---
# Write aggregated results to console for real-time monitoring
# Note: Console sink doesn't require checkpoint locations
# TODO: Add HBase or HDFS sink for persistence when VM HDFS configuration is ready
#
# Output Mode: "update" - Only changed aggregations are output
# Trigger: Process every 5 seconds for near real-time results

print(">>> Starting Advanced Stream Processing...")

# Start Binance aggregation output stream
query_binance = binance_agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

# Start Polymarket aggregation output stream
query_poly = poly_agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

# NOTE ON PROCESSING TRIGGER:
# Processing trigger is set to 5 seconds for responsiveness while windowed aggregations
# operate on 1-minute tumbling windows. This design provides frequent micro-batch updates
# that reflect recent data changes without waiting for the full 1-minute window to complete.
# Trade-off: 5-second trigger ensures timely feedback (responsive dashboards/alerting) but
# creates more micro-batches. For production systems optimizing for throughput, consider
# aligning trigger with window duration (.trigger(processingTime="1 minute")) to reduce
# state management overhead. For latency-critical use cases, 5 seconds is appropriate.

print(">>> All streaming queries started successfully.")
print(">>> Monitoring for failures...")

# --- 7. MONITORING & ERROR HANDLING ---
# Monitor all active streams and handle failures gracefully
try:
    # Wait for any stream to terminate (normal or error)
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n>>> Received interrupt signal. Shutting down gracefully...")
    # Stop all streaming queries gracefully
    for query in spark.streams.active:
        print(f">>> Stopping query: {query.name or query.id}")
        query.stop()
    print(">>> All streams stopped. Exiting.")
except Exception as e:
    print(f"\n>>> ERROR: Stream processing failed with exception:")
    print(f">>> {type(e).__name__}: {str(e)}")
    
    # Print status of all active streams for debugging
    print("\n>>> Active Stream Status:")
    for query in spark.streams.active:
        print(f"  - Query: {query.name or query.id}")
        print(f"    Status: {query.status}")
        if query.lastProgress:
            print(f"    Last Progress: {query.lastProgress}")
    
    # Attempt graceful shutdown
    print("\n>>> Attempting graceful shutdown...")
    for query in spark.streams.active:
        try:
            query.stop()
        except:
            pass
    
    # Re-raise exception for proper error reporting
    raise
finally:
    print(">>> Stream processing terminated.")
