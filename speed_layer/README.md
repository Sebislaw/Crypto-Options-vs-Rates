# Speed Layer

The **Speed Layer** implements real-time stream processing for the Crypto Options vs Rates project, following the Lambda Architecture pattern. It provides near real-time analytics on cryptocurrency prices from Binance and prediction market data from Polymarket by consuming streaming data from Kafka topics and computing windowed aggregations using Apache Spark Structured Streaming.

---

## Overview

The Speed Layer bridges the gap between data ingestion and the batch layer by:
- **Processing streaming data in near real-time** (sub-second to seconds latency)
- **Computing incremental views** that complement batch-processed historical data
- **Enabling live monitoring** of market conditions and trading activity
- **Buffering recent data** until it's processed by the batch layer

### Architecture Position

```
┌─────────────────────┐
│  Ingestion Layer    │
│  (Binance/Polymarket)│
└──────────┬──────────┘
           │
           ▼
    ┌──────────────┐
    │    Kafka     │ ◄─── Message Broker
    └──────┬───────┘
           │
           ▼
    ┌──────────────┐
    │ Speed Layer  │ ◄─── YOU ARE HERE
    │ (Spark Stream)│
    └──────┬───────┘
           │
           ▼
    ┌──────────────┐
    │Serving Layer │
    │   (HBase)    │
    └──────────────┘
```

---

## Components

### 1. **Kafka** (`kafka/`)
Message broker infrastructure for buffering streaming data between ingestion and processing layers.

**Key File:**
- [`setup_kafka.sh`](kafka/setup_kafka.sh) - Deployment script for Kafka services and topic creation

**Topics Created:**
| Topic Name | Source | Content |
|------------|--------|---------|
| `binance` | Binance Collector | Real-time cryptocurrency price/volume data (1-minute klines) |
| `polymarket_trade` | Polymarket WebSocket | Prediction market trades and order book snapshots |
| `polymarket_metadata` | Polymarket Collector | Market metadata (descriptions, outcomes, resolution data) |

**Configuration:**
- **Zookeeper Port:** 2181
- **Kafka Broker Port:** 9092
- **Replication Factor:** 1 (suitable for single-node development)
- **Partitions:** 1 per topic (can be increased for higher throughput)

### 2. **Spark Streaming** (`spark/`)
Real-time data processing engine that consumes from Kafka, transforms data, and computes windowed aggregations.

**Key Files:**
- [`spark_streaming.py`](spark/spark_streaming.py) - Main Spark Structured Streaming job
- [`setup_spark.sh`](spark/setup_spark.sh) - Directory setup for checkpointing

**Processing Features:**
- Schema validation and JSON parsing
- Real-time metric computation (sentiment, imbalance, volatility)
- 1-minute tumbling window aggregations
- Watermarking for late data handling (2-minute grace period)
- Fault-tolerant checkpointing to HDFS

---

## Data Flow

```
Binance API          Polymarket WebSocket
     │                      │
     ▼                      ▼
[Ingestion Collectors]  [Ingestion Collectors]
     │                      │
     └──────┬───────────────┘
            │
            ▼
     ┌─────────────┐
     │   Kafka     │
     │   Topics    │
     └──────┬──────┘
            │
            ▼
   ┌────────────────┐
   │ Spark Streaming│
   │   Processing   │
   └────────┬───────┘
            │
            ├─► Console Output (Development)
            │
            └─► HBase/HDFS (Production)
```

---

## Quick Start

### Prerequisites

1. **Running VM** with VirtualBox port forwarding configured (see [`deployment/README.md`](../deployment/README.md))
2. **Kafka installed** at `/usr/local/kafka`
3. **Spark installed** with `spark-submit` available in PATH
4. **Python environment** configured via `setup_conda.sh`

### Initial Setup (First Time Only)

Run the project initialization script, which automatically sets up Kafka, Spark, and all required directories:

```bash
cd ~/Crypto-Options-vs-Rates
console_scripts/initialize_project.sh
```

This script will:
- Create HDFS directories
- Deploy Kafka and create topics
- Configure Spark checkpoint directories
- Deploy NiFi flows

### Starting the Speed Layer

Use the unified start script to launch ingestion collectors and Spark streaming simultaneously:

```bash
console_scripts/start_ingestion.sh
```

**What this does:**
1. Starts Binance collector (writes to Kafka `binance` topic)
2. Starts Polymarket WebSocket collector (writes to Kafka `polymarket_trade` topic)
3. Launches Spark Structured Streaming job (consumes from Kafka topics)

**Check logs:**
```bash
# View Spark streaming output
tail -f ~/Crypto-Options-vs-Rates/logs/spark_speed_layer.log

# View Binance collector logs
tail -f ~/Crypto-Options-vs-Rates/logs/binance_app.log

# View Polymarket collector logs
tail -f ~/Crypto-Options-vs-Rates/logs/polymarket_app.log
```

### Stopping the Speed Layer

```bash
console_scripts/stop_ingestion.sh
```

This gracefully stops all collectors and the Spark streaming job.

---

## Manual Operations

### Start Kafka Only

```bash
cd ~/Crypto-Options-vs-Rates
speed_layer/kafka/setup_kafka.sh
```

### Run Spark Streaming Manually

```bash
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
    ~/Crypto-Options-vs-Rates/speed_layer/spark/spark_streaming.py
```

### Monitor Kafka Topics

```bash
# List all topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# View messages from binance topic (Ctrl+C to stop)
kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic binance \
    --from-beginning

# View messages from polymarket_trade topic
kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic polymarket_trade \
    --from-beginning
```

### Check Kafka Topic Statistics

```bash
# Describe binance topic
kafka-topics.sh --describe \
    --bootstrap-server localhost:9092 \
    --topic binance

# Check consumer lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

---

## Metrics & Aggregations

### Binance Metrics (Cryptocurrency Markets)

**Input Schema:**
- `symbol`: Trading pair (e.g., SOLUSDT, BTCUSDT)
- `snapshot_time`: Event timestamp (milliseconds)
- `close`: Closing price for the interval
- `volume`: Base asset volume (e.g., SOL traded)
- `quote_asset_volume`: Quote asset volume (USDT value)
- `taker_buy_quote`: Volume of aggressive buy orders (USDT)

**Computed Metrics (1-minute windows):**
| Metric | Description | Use Case |
|--------|-------------|----------|
| `avg_price` | Mean price over window | Price level tracking |
| `volatility` | Standard deviation of price | Risk assessment |
| `total_usdt_volume` | Total USDT volume traded | Liquidity measurement |
| `avg_sentiment` | Buy pressure ratio (0-1) | Market sentiment indicator |
| `ticks` | Number of updates received | Data quality check |

**Buy Sentiment Calculation:**
```python
buy_sentiment = taker_buy_quote / quote_asset_volume
```
- **0.5** = Neutral (equal buying/selling)
- **> 0.5** = Bullish (more aggressive buyers)
- **< 0.5** = Bearish (more aggressive sellers)

### Polymarket Metrics (Prediction Markets)

**Input Schema:**
- `market_id`: Unique market identifier
- `timestamp`: Event timestamp (milliseconds)
- `bids`: Array of bid orders [{price, size}]
- `asks`: Array of ask orders [{price, size}]
- `price`: Trade execution price (for trade events)
- `size`: Trade execution size

**Computed Metrics (1-minute windows):**
| Metric | Description | Use Case |
|--------|-------------|----------|
| `avg_prob` | Average probability (mid-price) | Market expectation |
| `avg_imbalance` | Order book imbalance ratio | Buying/selling pressure |
| `avg_spread` | Average bid-ask spread | Liquidity indicator |
| `total_shares_traded` | Total volume traded | Activity level |
| `num_trades` | Number of trades executed | Market depth |

**Order Book Imbalance Calculation:**
```python
imbalance = (bid_depth - ask_depth) / (bid_depth + ask_depth)
```
- **+1** = All liquidity on bid side (strong buy pressure)
- **-1** = All liquidity on ask side (strong sell pressure)
- **0** = Balanced order book

---

## Configuration

### Kafka Configuration

Edit [`setup_kafka.sh`](kafka/setup_kafka.sh) to modify:
- Kafka installation path (`KAFKA_HOME`)
- Topic names
- Partition counts
- Replication factors

### Spark Streaming Configuration

Edit [`spark_streaming.py`](spark/spark_streaming.py) to adjust:

```python
# Kafka connection
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Checkpoint location (HDFS path for fault tolerance)
CHECKPOINT_LOCATION = "hdfs:///tmp/checkpoints"

# Window duration (currently 1 minute)
.groupBy(window(col("event_time"), "1 minute"), ...)

# Watermark delay (late data tolerance)
.withWatermark("event_time", "2 minutes")

# Processing trigger interval
.trigger(processingTime="5 seconds")
```

### Output Destinations

**Current:** Console output (for development/debugging)

**Production-Ready Options:**

1. **HBase** (recommended for serving layer):
```python
query = aggregated_data.writeStream \
    .outputMode("update") \
    .format("org.apache.hadoop.hbase.spark") \
    .option("hbase.table.name", "crypto_metrics") \
    .start()
```

2. **HDFS** (for persistence):
```python
query = aggregated_data.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs:///user/vagrant/speed_layer/output") \
    .option("checkpointLocation", "hdfs:///tmp/checkpoints") \
    .start()
```

3. **Kafka** (for downstream consumers):
```python
query = aggregated_data.writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "processed_metrics") \
    .start()
```

---

## Troubleshooting

### Kafka Issues

**Problem:** Zookeeper or Kafka won't start
```bash
# Check if ports are already in use
netstat -tuln | grep -E '2181|9092'

# Kill existing processes
sudo pkill -f zookeeper
sudo pkill -f kafka

# Restart
speed_layer/kafka/setup_kafka.sh
```

**Problem:** Topics not created
```bash
# Manually create missing topics
kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 1 \
    --topic binance
```

### Spark Streaming Issues

**Problem:** `py4j` or Kafka connector errors
```bash
# Ensure Kafka connector package is included
spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
    speed_layer/spark/spark_streaming.py
```

**Problem:** HDFS checkpoint errors
```bash
# Create checkpoint directory
hdfs dfs -mkdir -p /tmp/checkpoints
hdfs dfs -chmod 777 /tmp/checkpoints

# Or use local filesystem for testing
# Edit spark_streaming.py:
CHECKPOINT_LOCATION = "file:///tmp/checkpoints"
```

**Problem:** No data appearing in stream
```bash
# 1. Check if Kafka topics have data
kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic binance \
    --max-messages 5

# 2. Verify ingestion collectors are running
ps aux | grep -E 'binance|polymarket'

# 3. Check collector logs
tail -f ~/Crypto-Options-vs-Rates/logs/binance_error.log
```

**Problem:** High memory usage
```bash
# Reduce shuffle partitions in spark_streaming.py:
.config("spark.sql.shuffle.partitions", "2")

# Limit processing rate:
.option("maxOffsetsPerTrigger", "1000")
```

---

## Performance Tuning

### Optimize for Low Latency
```python
# Reduce processing interval
.trigger(processingTime="1 second")

# Decrease shuffle partitions
.config("spark.sql.shuffle.partitions", "2")

# Use smaller watermark
.withWatermark("event_time", "30 seconds")
```

### Optimize for High Throughput
```python
# Increase parallelism
.config("spark.sql.shuffle.partitions", "8")

# Batch more records
.trigger(processingTime="30 seconds")

# Increase Kafka fetch size
.option("kafka.fetch.max.bytes", "52428800")  # 50MB
```

---

## Integration with Other Layers

### Ingestion Layer
- **Binance Collector** ([`ingestion_layer/binance/`](../ingestion_layer/binance/)) writes kline data to Kafka `binance` topic
- **Polymarket WebSocket** ([`ingestion_layer/polymarket/`](../ingestion_layer/polymarket/)) streams order book updates to `polymarket_trade` topic

### Batch Layer
- Historical data stored in HDFS is processed by Spark batch jobs ([`batch_layer/spark/`](../batch_layer/spark/))
- Hive tables ([`batch_layer/hive/`](../batch_layer/hive/)) provide structured access to historical views

### Serving Layer
- Real-time metrics from Speed Layer are merged with batch views
- HBase ([`serving_layer/hbase/`](../serving_layer/hbase/)) serves unified low-latency queries
- Merge logic ([`serving_layer/merge/`](../serving_layer/merge/)) reconciles real-time and batch data

---

## Monitoring & Debugging

### Spark UI

Access Spark Web UI while job is running:
```
http://localhost:4040
```

**Available Tabs:**
- **Jobs:** Progress of streaming batches
- **Stages:** Task-level execution details
- **SQL:** Physical plans and query statistics
- **Streaming:** Batch processing rates and watermark status

### Key Metrics to Monitor

1. **Processing Rate:** Ensure Spark keeps up with ingestion rate
2. **Batch Duration:** Should be < trigger interval (5 seconds)
3. **Watermark:** Check for excessive late data
4. **Kafka Lag:** Consumer offset vs latest offset
5. **Memory Usage:** Watch for OOM errors

### Logging

**Adjust log level in [`spark_streaming.py`](spark/spark_streaming.py):**
```python
# Less verbose (default)
spark.sparkContext.setLogLevel("WARN")

# Debug mode (verbose)
spark.sparkContext.setLogLevel("INFO")

# Minimal output
spark.sparkContext.setLogLevel("ERROR")
```

---

## Testing

### Unit Tests

Run unit tests for utility functions:
```bash
cd ~/Crypto-Options-vs-Rates
python -m pytest tests/unit_tests/
```

### Functional Testing

See [`tests/functional_tests/`](../tests/functional_tests/) for end-to-end test scenarios.

### Manual Smoke Test

1. Start Kafka: `speed_layer/kafka/setup_kafka.sh`
2. Produce test message:
```bash
echo '{"symbol": "TESTUSDT", "close": 100.0, "volume": 1000.0}' | \
kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic binance
```
3. Start Spark streaming: `spark-submit speed_layer/spark/spark_streaming.py`
4. Verify output appears in console

---

## Architecture Decisions

### Why Kafka?
- **Decoupling:** Separates ingestion from processing
- **Buffering:** Handles burst traffic and downstream slowdowns
- **Replay:** Can reprocess data from specific offsets
- **Scalability:** Easy to add more consumers/partitions

### Why Spark Structured Streaming?
- **Unified API:** Same DataFrame/SQL API as batch processing
- **Exactly-once:** Built-in support via checkpointing
- **Late data:** Watermarking handles out-of-order events
- **Integration:** Native Kafka and HDFS connectors

### Why 1-Minute Windows?
- **Balance:** Good trade-off between latency and statistical significance
- **Alignment:** Matches Binance kline intervals
- **Freshness:** Recent enough for trading decisions
- **Overhead:** Manageable state size and update frequency

---

## Future Enhancements

- [ ] Write aggregations to HBase for serving layer
- [ ] Add alerting for anomalous market conditions
- [ ] Implement dynamic scaling based on ingestion rate
- [ ] Add multi-asset correlation analysis
- [ ] Create Grafana dashboards for real-time monitoring
- [ ] Implement backpressure handling
- [ ] Add data quality metrics and validation rules

---

## References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Lambda Architecture](http://lambda-architecture.net/)
- [Main Project README](../README.md)

---

## Support

For issues or questions:
1. Check logs in `~/Crypto-Options-vs-Rates/logs/`
2. Review [Troubleshooting](#troubleshooting) section above
3. Consult [`deployment/README.md`](../deployment/README.md) for VM setup
4. See [`tests/functional_tests/`](../tests/functional_tests/) for test cases

---

**Last Updated:** 2026-01-11
