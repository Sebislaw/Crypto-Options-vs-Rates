from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, current_timestamp, array_max, array_min, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType

# --- 1. Initialize Spark Session ---
spark = SparkSession.builder \
    .appName("CryptoRatesVsOptions_SpeedLayer") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

# Reduce log noise
spark.sparkContext.setLogLevel("WARN")

# --- 2. Define Precise Schemas ---

# A. Binance Schema (Single Record Definition)
binance_record_schema = StructType([
    StructField("timestamp", LongType(), True),
    StructField("symbol", StringType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("date", StringType(), True)
])

# B. Polymarket Trade Schema (Superset for 'book', 'price_change', etc.)
# Note: Prices in your JSON are Strings ("0.49"), so we define them as StringType first
order_item = StructType([
    StructField("price", StringType(), True),
    StructField("size", StringType(), True)
])

poly_trade_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("market", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("bids", ArrayType(order_item), True),          # For 'book' events
    StructField("asks", ArrayType(order_item), True)           # For 'book' events
])

# --- 3. Read Streams from Kafka ---
def read_stream(topic):
    return spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

raw_binance = read_stream("binance")
raw_poly = read_stream("polymarket_trade")

# --- 4. Parse & Transform (The Critical Part) ---

# A. Binance: Handle JSON Array -> Explode -> Flat Rows
binance_df = raw_binance.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), ArrayType(binance_record_schema)).alias("data")) \
    .select(explode(col("data")).alias("record")) \
    .select(
        col("record.symbol"),
        col("record.close").alias("price"),
        col("record.timestamp"),
        current_timestamp().alias("ingest_time")
    )

# B. Polymarket: Handle String Prices & Calculate Probability
poly_df = raw_poly.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), poly_trade_schema).alias("data")) \
    .select("data.*") \
    .filter(col("bids").isNotNull()) \
    .select(
        col("market").alias("market_id"),
        col("timestamp"),
        # Transform String "0.49" -> Double 0.49 inside the arrays
        expr("transform(bids, x -> cast(x.price as double))").alias("bid_prices"),
        expr("transform(asks, x -> cast(x.price as double))").alias("ask_prices")
    ) \
    .withColumn("best_bid", array_max(col("bid_prices"))) \
    .withColumn("best_ask", array_min(col("ask_prices"))) \
    .withColumn("implied_probability", (col("best_bid") + col("best_ask")) / 2) \
    .select("market_id", "implied_probability", "best_bid", "best_ask", "timestamp")

# --- 5. Output to Console ---
print(">>> Starting Stream Processing...")

query1 = binance_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

query2 = poly_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

spark.streams.awaitAnyTermination()