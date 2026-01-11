from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, expr, current_timestamp, array_max, array_min, 
    explode, window, avg, sum, count, max, min, stddev, abs, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, ArrayType
)

# --- 1. CONFIGURATION ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
CHECKPOINT_LOCATION = "hdfs:///tmp/checkpoints" 

spark = SparkSession.builder \
    .appName("CryptoOptions_SpeedLayer") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 2. SCHEMAS ---

# A. Binance Schema (Added quote_asset_volume)
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
order_item = StructType([
    StructField("price", StringType(), True),
    StructField("size", StringType(), True)
])

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
    return spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

raw_binance = read_kafka("binance")
raw_poly = read_kafka("polymarket_trade")

# --- 4. TRANSFORMATIONS ---

# === BINANCE ===
binance_clean = raw_binance.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), ArrayType(binance_schema)).alias("data")) \
    .select(explode(col("data")).alias("r")) \
    .select(
        col("r.symbol"),
        (col("r.snapshot_time") / 1000).cast("timestamp").alias("event_time"),
        col("r.close").alias("price"),
        col("r.quote_asset_volume").alias("usdt_volume"), # NEW: Normalized Volume
        
        # Buying Sentiment (USDT): (Taker Buy USDT / Total USDT)
        # 0.5 = Neutral, >0.5 = Bullish, <0.5 = Bearish
        (col("r.taker_buy_quote") / col("r.quote_asset_volume")).alias("buy_sentiment")
    )

# === POLYMARKET ===
# Calculate Order Book Imbalance
poly_clean = raw_poly.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), poly_schema).alias("data")) \
    .select(
        col("data.market").alias("market_id"),
        col("data.event_type"),
        (col("data.timestamp").cast("long") / 1000).cast("timestamp").alias("event_time"),
        
        # Extract Top Bid/Ask Prices
        array_max(expr("transform(data.bids, x -> cast(x.price as double))")).alias("best_bid"),
        array_min(expr("transform(data.asks, x -> cast(x.price as double))")).alias("best_ask"),
        
        # NEW: Extract Top Bid/Ask Sizes for Imbalance
        # Note: This takes the size of the *best* price level only for simplicity
        expr("filter(data.bids, x -> x.price == array_max(transform(data.bids, y -> y.price)))[0].size").cast("double").alias("bid_depth"),
        expr("filter(data.asks, x -> x.price == array_min(transform(data.asks, y -> y.price)))[0].size").cast("double").alias("ask_depth"),
        
        # Trade Info
        col("data.size").cast("double").alias("trade_size"),
        col("data.price").cast("double").alias("trade_price")
    ) \
    .withColumn("spread", abs(col("best_ask") - col("best_bid"))) \
    .withColumn("mid_price_prob", (col("best_bid") + col("best_ask")) / 2) \
    .withColumn("book_imbalance", 
                (col("bid_depth") - col("ask_depth")) / (col("bid_depth") + col("ask_depth"))
    )

# --- 5. AGGREGATIONS (1-Minute Windows) ---

# BINANCE
binance_agg = binance_clean \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(window(col("event_time"), "1 minute"), col("symbol")) \
    .agg(
        avg("price").alias("avg_price"),
        stddev("price").alias("volatility"),
        sum("usdt_volume").alias("total_usdt_volume"), # NEW: Dollar Volume
        avg("buy_sentiment").alias("avg_sentiment"),
        count("*").alias("ticks")
    )

# POLYMARKET
poly_agg = poly_clean \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(window(col("event_time"), "1 minute"), col("market_id")) \
    .agg(
        avg("mid_price_prob").alias("avg_prob"),
        avg("book_imbalance").alias("avg_imbalance"),   # NEW: Pressure Metric
        avg("spread").alias("avg_spread"),
        sum("trade_size").alias("total_shares_traded"),
        count("trade_price").alias("num_trades")
    )

# --- 6. OUTPUT ---

print(">>> Starting Advanced Stream Processing...")

query_binance = binance_agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

query_poly = poly_agg.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

spark.streams.awaitAnyTermination()