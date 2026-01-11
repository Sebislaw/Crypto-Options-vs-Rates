# Batch Layer Implementation Summary

## 📦 What Was Implemented

A complete **Batch Layer** for the Lambda Architecture that analyzes correlations between Polymarket prediction markets and Binance cryptocurrency spot prices.

---

## 📂 Files Created

### Hive (Data Warehouse)
- **`batch_layer/hive/create_tables.sql`** - External table definitions for Binance and Polymarket data
- **`batch_layer/hive/repair_partitions.sql`** - Partition discovery and verification

### Spark (Batch Processing)
- **`batch_layer/spark/batch_analytics.py`** - Main PySpark job (15-min window aggregation, join, analysis)
- **`batch_layer/spark/config.py`** - Configuration parameters (thresholds, paths, HBase settings)
- **`batch_layer/spark/query_hbase.py`** - HBase query utility for viewing results

### HBase (Serving Layer Storage)
- **`batch_layer/hbase/create_table.sh`** - Table creation script with column families

### Documentation & Utilities
- **`batch_layer/README.md`** - Comprehensive documentation
- **`batch_layer/QUICKSTART.md`** - Quick start guide
- **`batch_layer/verify_environment.sh`** - Environment verification script
- **`batch_layer/run_batch_analytics.sh`** - One-command execution script
- **`batch_layer/hadoop/hdfs_directory_structure.md`** - HDFS layout documentation

---

## 🎯 Core Functionality

### Data Sources (Hive)
1. **binance_klines** - 1-minute OHLC candles from Binance
   - Partitioned by date
   - Schema: timestamp, open, high, low, close, volume, symbol, etc.

2. **polymarket_orderbook** - Real-time orderbook data
   - Partitioned by date
   - Schema: market, asset_id, timestamp, last_trade_price (probability), bids/asks

### Processing Logic (Spark)
1. **Load & Clean**: Read from Hive, parse timestamps, map symbols
2. **Aggregate**: Create 15-minute windows for both datasets
   - Binance: OHLC aggregation
   - Polymarket: Average probability, max probability, activity count
3. **Join**: Match on window timestamp + crypto symbol
4. **Analyze**: Compute correlation metrics
   - Price movement: `(close - open) / open`
   - Prediction accuracy: Did high probability predict correct direction?
5. **Write**: Store in HBase with proper row key design

### Output Schema (HBase: market_analytics)
- **Row Key**: `{SYMBOL}_{REVERSE_TIMESTAMP}` (e.g., `BTCUSDT_9999999998232799600`)
- **Column Families**:
  - `price_data`: open, close, high, low, volume
  - `bet_data`: avg_prob, max_prob, activity
  - `analysis`: price_movement, actual_direction, predicted_direction, result

### Prediction Logic
```python
if avg_probability > 0.60:
    prediction = "UP"
elif avg_probability < 0.40:
    prediction = "DOWN"
else:
    prediction = "UNCERTAIN"

# Compare with actual price movement
if prediction == "UP" and price went up:
    result = "CORRECT_BULL" ✅
elif prediction == "UP" and price went down:
    result = "FAILED_BULL" ❌
...
```

---

## 🚀 How to Run

### Quick Start (One Command)
```bash
cd ~/Crypto-Options-vs-Rates
chmod +x batch_layer/run_batch_analytics.sh
batch_layer/run_batch_analytics.sh
```

### Verify Environment First
```bash
chmod +x batch_layer/verify_environment.sh
bash batch_layer/verify_environment.sh
```

### Manual Steps (For Learning)
```bash
# 1. Create Hive tables
hive -f batch_layer/hive/create_tables.sql
hive -f batch_layer/hive/repair_partitions.sql

# 2. Create HBase table
bash batch_layer/hbase/create_table.sh

# 3. Run Spark job
spark-submit \
    --master local[*] \
    --driver-memory 2g \
    batch_layer/spark/batch_analytics.py

# 4. View results
python batch_layer/spark/query_hbase.py
```

---

## 📊 Expected Results

### Console Output
```
[1/6] Loading Binance data from Hive...
      Loaded 5760 Binance records

[2/6] Loading Polymarket data from Hive...
      Loaded 3241 Polymarket records

[3/6] Aggregating to 15-minute windows...
      Binance windows: 96
      Polymarket windows: 96

[4/6] Joining data and computing analytics...
      Generated 84 analysis records

[6/6] Writing to HBase...
      Successfully wrote to HBase!

ANALYSIS SUMMARY
+-----------------+-----+
|prediction_result|count|
+-----------------+-----+
|CORRECT_BULL     |23   |
|FAILED_BULL      |12   |
|CORRECT_BEAR     |8    |
|UNCERTAIN        |41   |
+-----------------+-----+
```

### HBase Storage
```bash
# View results
echo 'scan "market_analytics", {LIMIT => 5}' | hbase shell
```

Sample output:
```
BTCUSDT_9999999998232799600
  price_data:open => 94532.10
  price_data:close => 94621.45
  bet_data:avg_prob => 0.62
  analysis:result => CORRECT_BULL
  analysis:price_movement => 0.000944
```

---

## ✅ Project Requirements Met

| Requirement | Implementation | Points |
|-------------|----------------|--------|
| **Data Storage** | HBase with proper schema (3 column families) | 2 |
| **Batch Analysis** | PySpark with windowing, joins, aggregations | 5 |
| **Hive Integration** | External tables on HDFS Parquet data | ✓ |
| **Documentation** | Comprehensive README + Quick Start | ✓ |

---

## 🔧 Technical Highlights

### Scalability
- Partitioned Hive tables (by date)
- Window-based aggregation (reduces data volume)
- HBase row key design optimized for range scans

### Data Quality
- Timestamp normalization (milliseconds → datetime)
- String-to-numeric conversions with null handling
- Symbol mapping (btc → BTCUSDT)

### Fault Tolerance
- Fallback to Parquet backup if HBase write fails
- Comprehensive error handling and logging
- Environment verification before execution

### Performance
- Data caching during aggregation
- Batch writes to HBase (100 records/batch)
- SNAPPY compression in both Hive and HBase

---

## 🔍 Key Insights from Analysis

The batch job computes:
1. **Prediction accuracy rate**: % of correct predictions vs failed
2. **Market sentiment correlation**: How well prediction probability aligns with price movement
3. **Symbol-specific performance**: Which crypto predictions are most accurate
4. **Time-series analysis**: 15-minute granularity for pattern detection

---

## 📁 Integration with Lambda Architecture

```
Ingestion Layer (Python collectors)
        ↓
    Raw HDFS (/user/vagrant/raw/)
        ↓
    NiFi (cleansing)
        ↓
    Cleansed HDFS (/user/vagrant/cleansed/)
        ↓
    ┌─────────────────┬─────────────────┐
    │                 │                 │
Batch Layer      Speed Layer    Serving Layer
(This impl)     (Kafka+Spark)    (HBase+Merge)
    │                 │                 │
    └─────────────────┴─────────────────┘
                Dashboard
```

---

## 🛠️ Dependencies

All required packages in `requirements/python_requirements.txt`:
- `pyspark` (included with Spark)
- `happybase==1.2.0` (HBase client)
- `pandas`, `pyarrow` (data processing)

Additional install if needed:
```bash
pip install happybase
```

---

## 📝 Configuration Options

Edit `batch_layer/spark/config.py`:

```python
# Analysis thresholds
PREDICTION_THRESHOLD = 0.60  # Probability for "bullish"
MIN_PRICE_MOVEMENT = 0.001   # 0.1% minimum change

# Window settings
WINDOW_DURATION = '15 minutes'

# HBase connection
HBASE_HOST = 'localhost'
HBASE_PORT = 9090
```

---

## 🐛 Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| No data in Hive tables | Run `hive -f batch_layer/hive/repair_partitions.sql` |
| HBase connection failed | Start Thrift: `hbase-daemon.sh start thrift` |
| Empty join results | Check date ranges match in both tables |
| Spark OOM errors | Increase memory: `--driver-memory 4g` |

---

## 📚 Additional Resources

- **Full Documentation**: `batch_layer/README.md`
- **Quick Start**: `batch_layer/QUICKSTART.md`
- **HDFS Layout**: `batch_layer/hadoop/hdfs_directory_structure.md`
- **Verification Tool**: `batch_layer/verify_environment.sh`

---

## 🎓 Learning Outcomes

This implementation demonstrates:
- Lambda Architecture batch layer design
- Hive external tables on Parquet data
- PySpark window functions and aggregations
- Complex join operations with multiple conditions
- HBase schema design and row key optimization
- Integration of multiple Hadoop ecosystem components
- Production-ready error handling and logging

---

## 🚀 Next Steps

1. **Run the batch job** on current data
2. **Verify results** in HBase
3. **Analyze accuracy metrics** for different symbols
4. **Optional**: Extend with additional metrics (volatility, volume correlation, etc.)
5. **Integration**: Connect to dashboard/serving layer

---

**Implementation Status**: ✅ Complete and ready for execution  
**Code Quality**: Production-ready with error handling and documentation  
**Testing**: Includes verification script and query utilities

---

Good luck with your project! 🎉
