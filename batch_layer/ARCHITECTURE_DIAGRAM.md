# Batch Layer Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA INGESTION LAYER                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────┐              ┌──────────────────────┐                │
│  │ Binance Collect │              │ Polymarket Collector │                │
│  │  (Python)       │              │   (WebSocket)        │                │
│  │  - 1min klines  │              │   - Orderbook data   │                │
│  │  - 4 symbols    │              │   - 4 cryptos        │                │
│  └────────┬────────┘              └──────────┬───────────┘                │
│           │                                   │                            │
│           └───────────────┬───────────────────┘                            │
│                           ▼                                                │
│           ┌───────────────────────────────┐                                │
│           │  HDFS: /user/vagrant/raw/     │                                │
│           │  - binance/                   │                                │
│           │  - polymarket_trade/          │                                │
│           │  - polymarket_metadata/       │                                │
│           └──────────────┬────────────────┘                                │
│                          │                                                 │
└──────────────────────────┼─────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          NiFi PROCESSING LAYER                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌────────────────────────────────────────────────────────────┐            │
│  │  NiFi Flow:                                                │            │
│  │  1. ListHDFS (monitor raw directories)                    │            │
│  │  2. FetchHDFS (read files)                                 │            │
│  │  3. ConvertRecord (JSON/Parquet → unified Parquet)         │            │
│  │  4. PutHDFS (write to cleansed/)                           │            │
│  │  5. PublishKafka (stream to speed layer)                  │            │
│  └────────────────────────┬───────────────────────────────────┘            │
│                           │                                                │
│                           ▼                                                │
│  ┌────────────────────────────────────────────────────────┐               │
│  │  HDFS: /user/vagrant/cleansed/                         │               │
│  │  ├─ binance/date=YYYY-MM-DD/*.parquet                  │               │
│  │  ├─ polymarket_trade/date=YYYY-MM-DD/*.parquet         │               │
│  │  └─ polymarket_metadata/date=YYYY-MM-DD/*.parquet      │               │
│  └────────────────────────┬───────────────────────────────┘               │
│                           │                                                │
└───────────────────────────┼────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           BATCH LAYER (YOUR WORK)                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  STEP 1: HIVE METADATA LAYER                                               │
│  ┌─────────────────────────────────────────────────────────────┐           │
│  │  CREATE EXTERNAL TABLE binance_klines                       │           │
│  │  LOCATION '/user/vagrant/cleansed/binance'                  │           │
│  │  PARTITIONED BY (date)                                       │           │
│  │                                                              │           │
│  │  CREATE EXTERNAL TABLE polymarket_orderbook                 │           │
│  │  LOCATION '/user/vagrant/cleansed/polymarket_trade'         │           │
│  │  PARTITIONED BY (date)                                       │           │
│  │                                                              │           │
│  │  MSCK REPAIR TABLE (discover partitions)                    │           │
│  └──────────────────────────┬──────────────────────────────────┘           │
│                             │                                              │
│                             ▼                                              │
│  STEP 2: SPARK BATCH PROCESSING                                            │
│  ┌─────────────────────────────────────────────────────────────┐           │
│  │  batch_analytics.py                                         │           │
│  │                                                              │           │
│  │  A. Load from Hive:                                         │           │
│  │     ├─ SELECT * FROM binance_klines                         │           │
│  │     └─ SELECT * FROM polymarket_orderbook                   │           │
│  │                                                              │           │
│  │  B. Parse & Clean:                                          │           │
│  │     ├─ Convert timestamps (ms → datetime)                   │           │
│  │     ├─ Map symbols (btc → BTCUSDT)                          │           │
│  │     └─ Cast types (string → double)                         │           │
│  │                                                              │           │
│  │  C. Aggregate to 15-min Windows:                            │           │
│  │     ┌──────────────────┐    ┌──────────────────┐            │           │
│  │     │ Binance:         │    │ Polymarket:      │            │           │
│  │     │ - OHLC           │    │ - Avg prob       │            │           │
│  │     │ - Volume         │    │ - Max prob       │            │           │
│  │     │ - Candle count   │    │ - Activity cnt   │            │           │
│  │     └────────┬─────────┘    └─────┬────────────┘            │           │
│  │              │                    │                         │           │
│  │              └──────────┬─────────┘                         │           │
│  │                         ▼                                   │           │
│  │  D. Join on (window_start, crypto):                         │           │
│  │     window_start=12:00, crypto=btc                          │           │
│  │                                                              │           │
│  │  E. Compute Metrics:                                        │           │
│  │     ├─ price_movement = (close-open)/open                   │           │
│  │     ├─ actual_direction = UP/DOWN/FLAT                      │           │
│  │     ├─ predicted_direction = UP/DOWN/UNCERTAIN              │           │
│  │     └─ prediction_result = CORRECT_BULL/FAILED_BULL/...     │           │
│  │                                                              │           │
│  │  F. Prepare HBase Format:                                   │           │
│  │     ├─ rowkey = SYMBOL_REVERSE_TIMESTAMP                    │           │
│  │     └─ flatten to column families                           │           │
│  └──────────────────────────┬──────────────────────────────────┘           │
│                             │                                              │
│                             ▼                                              │
│  STEP 3: HBASE STORAGE (SERVING LAYER)                                     │
│  ┌─────────────────────────────────────────────────────────────┐           │
│  │  Table: market_analytics                                    │           │
│  │                                                              │           │
│  │  Row Key: BTCUSDT_9999999998232799600                       │           │
│  │    ├─ price_data:open       = 94532.10                      │           │
│  │    ├─ price_data:close      = 94621.45                      │           │
│  │    ├─ price_data:high       = 94650.00                      │           │
│  │    ├─ price_data:low        = 94520.00                      │           │
│  │    ├─ price_data:volume     = 125.45                        │           │
│  │    ├─ bet_data:avg_prob     = 0.62                          │           │
│  │    ├─ bet_data:max_prob     = 0.68                          │           │
│  │    ├─ bet_data:activity     = 147                           │           │
│  │    ├─ analysis:price_movement = 0.000944                    │           │
│  │    ├─ analysis:actual_direction = UP                        │           │
│  │    ├─ analysis:predicted_direction = UP                     │           │
│  │    ├─ analysis:result       = CORRECT_BULL                  │           │
│  │    ├─ analysis:timestamp    = 2026-01-11 12:00:00           │           │
│  │    └─ analysis:symbol       = BTCUSDT                       │           │
│  └─────────────────────────────────────────────────────────────┘           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SERVING LAYER / DASHBOARD                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────┐           │
│  │  Query HBase:                                               │           │
│  │  - Get latest predictions by symbol                         │           │
│  │  - Calculate accuracy statistics                            │           │
│  │  - Visualize correlation trends                             │           │
│  └─────────────────────────────────────────────────────────────┘           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘


ANALYSIS LOGIC DETAIL:
═══════════════════════

Input Window (15 minutes: 12:00 - 12:15):
┌────────────────────┬─────────────────────┐
│ Binance            │ Polymarket          │
├────────────────────┼─────────────────────┤
│ Open:   94532.10   │ Avg Prob:  0.62     │
│ Close:  94621.45   │ (62% predict UP)    │
│ Movement: +0.094%  │                     │
└────────────────────┴─────────────────────┘
         │                      │
         └──────────┬───────────┘
                    ▼
            Analysis Logic:
            ───────────────
            Predicted: UP (prob > 0.6)
            Actual: UP (movement > 0)
            Result: CORRECT_BULL ✅


SYMBOL MAPPING:
═══════════════
Polymarket Market → Binance Symbol
────────────────────────────────────
btc               → BTCUSDT
eth               → ETHUSDT
sol               → SOLUSDT
xrp               → XRPUSDT


EXECUTION FLOW:
═══════════════

1. User runs: batch_layer/run_batch_analytics.sh
                      ↓
2. Script verifies: HDFS, Hive, HBase all running
                      ↓
3. Creates Hive tables (if needed)
                      ↓
4. Repairs partitions (discovers date folders)
                      ↓
5. Creates HBase table (if needed)
                      ↓
6. Submits Spark job
                      ↓
7. Spark reads from Hive, processes, writes to HBase
                      ↓
8. Results available for querying


TIME WINDOWS:
═════════════

Original Data:
├─ Binance: 1-minute candles
└─ Polymarket: Real-time orderbook (millisecond precision)

Aggregated to 15-minute windows:
12:00:00 ─┬─ Multiple 1-min candles
          ├─ Hundreds of orderbook snapshots
          │
12:15:00 ─┼─ Aggregated into single row:
          │  - Binance: OHLC for 12:00-12:15
          │  - Polymarket: Avg probability for 12:00-12:15
          │
          └─ Joined & Analyzed → Written to HBase


COLUMN FAMILY STRUCTURE:
════════════════════════

market_analytics table:

price_data (Binance spot data)
├─ open: double
├─ high: double
├─ low: double
├─ close: double
└─ volume: double

bet_data (Polymarket prediction data)
├─ avg_prob: double
├─ max_prob: double
└─ activity: int

analysis (Computed correlation)
├─ price_movement: double
├─ actual_direction: string
├─ predicted_direction: string
├─ result: string
├─ timestamp: string
├─ symbol: string
└─ crypto: string
```

---

**Execution Commands Summary:**

```bash
# One-liner (full automation)
batch_layer/run_batch_analytics.sh

# Or manual steps
hive -f batch_layer/hive/create_tables.sql
hive -f batch_layer/hive/repair_partitions.sql
bash batch_layer/hbase/create_table.sh
spark-submit --master local[*] batch_layer/spark/batch_analytics.py

# View results
python batch_layer/spark/query_hbase.py
```
