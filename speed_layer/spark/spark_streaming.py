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
    - Outputs both real-time snapshots and aggregate statistics
    - Results written to console (can be extended to HBase/HDFS)

Data Sources:
    - binance: Real-time cryptocurrency prices, volumes, and trading activity
    - polymarket_trade: Prediction market trades and order book snapshots

Output Metrics per 1-minute window:
    
    Binance (per symbol):
        Real-Time Snapshots:
            - current_price: Latest price in the window
            - current_sentiment: Latest buy sentiment ratio
        Aggregates:
            - avg_price: Mean price over window
            - min_price: Lowest price in window
            - max_price: Highest price in window
            - volatility: Standard deviation of price
            - total_usdt_volume: Total USDT volume traded
            - avg_sentiment: Average buy sentiment (0-1)
            - ticks: Number of data points
    
    Polymarket Order Books (per market):
        Real-Time Snapshots:
            - current_prob: Latest market probability
            - current_spread: Latest bid-ask spread
            - current_imbalance: Latest order book imbalance
        Aggregates:
            - avg_prob: Average probability over window
            - min_prob: Lowest probability in window
            - max_prob: Highest probability in window
            - avg_imbalance: Average order book pressure
            - avg_spread: Average bid-ask spread
            - num_updates: Number of order book updates
    
    Polymarket Trades (per market):
        Aggregates:
            - total_shares_traded: Total volume traded
            - num_trades: Number of trades executed

Author: Speed Layer Team
Last Modified: 2026-01-12
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, expr, array_max, array_min, 
    explode, window, avg, sum, count, stddev, abs, when, last, max, min
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, ArrayType
)

# --- 1. CONFIGURATION ---
# Kafka broker address for stream ingestion
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# HDFS checkpoint locations for fault-tolerant state management
# Each stream requires its own checkpoint directory to track progress independently
CHECKPOINT_BINANCE = "hdfs:///tmp/checkpoints/binance"
CHECKPOINT_POLY_BOOKS = "hdfs:///tmp/checkpoints/polymarket_books"
CHECKPOINT_POLY_TRADES = "hdfs:///tmp/checkpoints/polymarket_trades"

# Processing trigger interval for micro-batch processing (seconds)
# Lower values = more frequent updates but higher overhead
# Higher values = lower overhead but higher latency
PROCESSING_TRIGGER_INTERVAL = "5 seconds" 

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
# Note: The polymarket_trade topic contains TWO event types:
#   1. "book" events: Order book snapshots with bids/asks
#   2. "last_trade_price" events: Trade executions with price/size
# These must be processed separately to avoid null overwrites

# Parse raw Kafka messages and extract common fields
poly_parsed = raw_poly.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), poly_schema).alias("data")) \
    .select(
        col("data.market").alias("market_id"),
        col("data.event_type"),
        (col("data.timestamp").cast("long") / 1000).cast("timestamp").alias("event_time"),
        col("data.bids"),
        col("data.asks"),
        col("data.price"),
        col("data.size")
    )

# STREAM 1: Order Book Events ("book")
# Processes order book snapshots to calculate probability, spread, and imbalance
poly_books = poly_parsed \
    .filter(col("event_type") == "book") \
    .select(
        col("market_id"),
        col("event_time"),
        
        # Order Book Price Levels:
        # best_bid: Highest price buyers are willing to pay (max probability)
        # best_ask: Lowest price sellers are willing to accept (min probability)
        array_max(expr("transform(bids, x -> cast(x.price as double))")).alias("best_bid"),
        array_min(expr("transform(asks, x -> cast(x.price as double))")).alias("best_ask"),
        
        # Order Book Depth (Liquidity):
        # Extract the size (quantity) available at the best price levels
        # Used to calculate order book imbalance (bid pressure vs ask pressure)
        expr("element_at(filter(bids, x -> x.price == array_max(transform(bids, y -> y.price))), 1).size").cast("double").alias("bid_depth"),
        expr("element_at(filter(asks, x -> x.price == array_min(transform(asks, y -> y.price))), 1).size").cast("double").alias("ask_depth")
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

# STREAM 2: Trade Events ("last_trade_price")
# Processes trade executions to calculate volume and trade counts
poly_trades = poly_parsed \
    .filter(col("event_type") == "last_trade_price") \
    .select(
        col("market_id"),
        col("event_time"),
        col("price").cast("double").alias("trade_price"),
        col("size").cast("double").alias("trade_size")
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
        # --- Real-Time Snapshots (The "Now" State) ---
        last("price").alias("current_price"),
        last("buy_sentiment").alias("current_sentiment"),
        
        # --- Aggregates (The "Trend" State) ---
        avg("price").alias("avg_price"),                  # Mean price over window
        min("price").alias("min_price"),                  # Lowest price in window
        max("price").alias("max_price"),                  # Highest price in window
        stddev("price").alias("volatility"),             # Price volatility (std dev)
        sum("usdt_volume").alias("total_usdt_volume"),   # Total USDT traded
        avg("buy_sentiment").alias("avg_sentiment"),     # Average buying pressure
        count("*").alias("ticks")                        # Number of data points
    )

# POLYMARKET AGGREGATIONS
# Two separate aggregations for book events and trade events
# This prevents null overwrites that occur when processing mixed event types

# Aggregate 1: Order Book Metrics (from "book" events)
# Calculates market probability, liquidity metrics over order book snapshots
poly_books_agg = poly_books \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(window(col("event_time"), "1 minute"), col("market_id")) \
    .agg(
        # --- Real-Time Snapshots (The "Now" State) ---
        # Latest values from the order book at end of window
        last("mid_price_prob").alias("current_prob"),
        last("spread").alias("current_spread"),
        last("book_imbalance").alias("current_imbalance"),
        
        # --- Aggregates (The "Trend" State) ---
        # Statistical measures over all order book snapshots in the window
        avg("mid_price_prob").alias("avg_prob"),          # Average market probability
        min("mid_price_prob").alias("min_prob"),          # Lowest probability in window
        max("mid_price_prob").alias("max_prob"),          # Highest probability in window
        avg("book_imbalance").alias("avg_imbalance"),     # Average order book pressure
        avg("spread").alias("avg_spread"),                # Average bid-ask spread
        count("*").alias("num_updates")                   # Number of order book updates
    )

# Aggregate 2: Trade Metrics (from "last_trade_price" events)
# Calculates trading volume and trade counts
poly_trades_agg = poly_trades \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(window(col("event_time"), "1 minute"), col("market_id")) \
    .agg(
        sum("trade_size").alias("total_shares_traded"),   # Total trading volume
        count("*").alias("num_trades")                    # Number of trades executed
    )

# --- 6. OUTPUT SINKS ---
# Write aggregated results to HBase (persistent) and console (debug)
# Using foreachBatch to handle both outputs in a single micro-batch
#
# Three separate output streams:
#   1. Binance: Cryptocurrency price/volume metrics -> market_live:b
#   2. Polymarket Books: Order book probability/spread/imbalance -> market_live:p
#   3. Polymarket Trades: Trade volume and count metrics -> market_live:t
#
# Output Mode: "update" - Only changed aggregations are output
# Trigger: Process every 5 seconds for near real-time results

# Import HBase sink functions
try:
    from hbase_sink import (
        write_binance_batch,
        write_poly_books_batch,
        write_poly_trades_batch,
        set_hbase_enabled,
        test_connection
    )
    HBASE_AVAILABLE = True
except ImportError as e:
    print(f">>> WARNING: HBase sink not available: {e}")
    print(">>> Console-only output mode active.")
    HBASE_AVAILABLE = False
    
    # Fallback stub functions
    def write_binance_batch(df, batch_id): pass
    def write_poly_books_batch(df, batch_id): pass
    def write_poly_trades_batch(df, batch_id): pass

# Configuration: Enable/disable HBase writes
HBASE_WRITE_ENABLED = True
DEBUG_CONSOLE_ENABLED = True  # Keep console output for debugging


def write_with_console(hbase_writer, stream_name):
    """
    Create a foreachBatch function that writes to both HBase and console.
    
    Args:
        hbase_writer: HBase writer function (e.g., write_binance_batch)
        stream_name: Name for logging
        
    Returns:
        Function suitable for foreachBatch
    """
    def writer(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        
        # Write to HBase (if enabled)
        if HBASE_AVAILABLE and HBASE_WRITE_ENABLED:
            try:
                hbase_writer(batch_df, batch_id)
            except Exception as e:
                print(f">>> {stream_name} HBase write failed: {e}")
        
        # Write to console (if enabled) for debugging
        if DEBUG_CONSOLE_ENABLED:
            print(f"\n>>> {stream_name} Batch {batch_id}:")
            batch_df.show(truncate=False)
    
    return writer


print(">>> Starting Advanced Stream Processing...")
print(">>> Three output streams: Binance, Polymarket Books, Polymarket Trades")

# Test HBase connection at startup
if HBASE_AVAILABLE:
    print(">>> Testing HBase connection...")
    if test_connection():
        print(">>> HBase connection successful. Writing to market_live table.")
    else:
        print(">>> WARNING: HBase connection failed. Console-only mode.")
        HBASE_WRITE_ENABLED = False

# Start Binance aggregation output stream with HBase + console
query_binance = binance_agg.writeStream \
    .outputMode("update") \
    .foreachBatch(write_with_console(write_binance_batch, "Binance")) \
    .option("checkpointLocation", CHECKPOINT_BINANCE) \
    .trigger(processingTime=PROCESSING_TRIGGER_INTERVAL) \
    .start()

# Start Polymarket Order Book aggregation output stream
query_poly_books = poly_books_agg.writeStream \
    .outputMode("update") \
    .foreachBatch(write_with_console(write_poly_books_batch, "PolyBooks")) \
    .option("checkpointLocation", CHECKPOINT_POLY_BOOKS) \
    .trigger(processingTime=PROCESSING_TRIGGER_INTERVAL) \
    .start()

# Start Polymarket Trades aggregation output stream
query_poly_trades = poly_trades_agg.writeStream \
    .outputMode("update") \
    .foreachBatch(write_with_console(write_poly_trades_batch, "PolyTrades")) \
    .option("checkpointLocation", CHECKPOINT_POLY_TRADES) \
    .trigger(processingTime=PROCESSING_TRIGGER_INTERVAL) \
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
print(f">>> HBase writes: {'ENABLED' if HBASE_WRITE_ENABLED else 'DISABLED'}")
print(f">>> Console debug: {'ENABLED' if DEBUG_CONSOLE_ENABLED else 'DISABLED'}")
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
