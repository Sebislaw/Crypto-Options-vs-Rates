
# Binance Market Data Ingestion

This module provides automated ingestion, storage, and verification of Binance spot market kline (candlestick) data for selected crypto pairs. It supports both historical backfill and real-time streaming, storing all data in partitioned Parquet format for efficient analytics and downstream processing.

---

## Features

- **Automated historical backfill** for all configured symbols (default: BTCUSDT, ETHUSDT, SOLUSDT, XRPUSDT)
- **Real-time streaming** of 1-minute klines via Binance WebSocket
- **Efficient storage**: Partitioned Parquet files by symbol and date
- **Data verification**: Fast summary and sample queries using DuckDB
- **Configurable**: All settings in `config.py` (symbols, intervals, buffer size, etc.)

---

## Quickstart

### 1. Install Requirements

```bash
pip install -r requirements/python_requirements.txt
```

### 2. Configure (Optional)

Edit `binance-api/config.py` to change symbols, intervals, or storage location. By default, the following symbols are tracked:

```
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
```

### 3. Run Historical Backfill and Start Streaming

```bash
python binance-api/main.py
```

This will:
- Download the last 24 hours of 1-minute klines for each symbol
- Start a real-time WebSocket stream and append new klines as they arrive

---

## Data Storage Layout

All data is stored as Parquet files partitioned by symbol and date:

```
binance-api/data/
    symbol=BTCUSDT/
        date=2026-01-01/
            <file>.parquet
        date=2026-01-02/
            <file>.parquet
    symbol=ETHUSDT/
        ...
```

---

## Data Schema

Each row in the Parquet files contains the following columns:

| timestamp           | open      | high      | low       | close     | volume    | close_time      | quote_asset_volume | trades | taker_buy_base | taker_buy_quote | is_closed | snapshot_time    | date       | symbol   |
|---------------------|-----------|-----------|-----------|-----------|-----------|-----------------|-------------------|--------|----------------|-----------------|-----------|------------------|------------|----------|
| 2026-01-01 16:56:00 | 87972.06  | 87972.06  | 87972.06  | 87972.06  | 0.000000  | 1767286619999   | 0.000000          | 0      | 0.000000       | 0.000000        | True      | 1767286619999    | 2026-01-01 | BTCUSDT  |

---

## Data Verification & Exploration

### Summarize and Sample Data

```bash
python binance-api/verify_data.py
```

**Example output:**

```
Verifying data in C:\Users\sebas\Desktop\GitHubRepositories\Crypto-Options-vs-Rates\binance-api\data...
Found 20 parquet files.
Total records: 11532

Sample data (top 5):
                        timestamp      open      high       low     close     volume        close_time  quote_asset_volume  trades  taker_buy_base  taker_buy_quote  is_closed     snapshot_time        date   symbol
0 2026-01-01 16:56:00  87972.06  87972.06  87972.06  87972.06   0.000000  1767286619999        0.000000      0        0.000000        0.000000        True      1767286619999   2026-01-01  BTCUSDT
1 2026-01-01 16:57:00  87972.06  87972.06  87972.06  87972.06   0.000000  1767286679999        0.000000      0        0.000000        0.000000        True      1767286679999   2026-01-01  BTCUSDT
2 2026-01-01 16:58:00  87972.06  87986.75  87972.06  87986.75   0.000000  1767286739999        0.000000      0        0.000000        0.000000        True      1767286739999   2026-01-01  BTCUSDT
3 2026-01-01 16:59:00  87986.75  87988.77  87986.75  87988.77   0.000000  1767286799999        0.000000      0        0.000000        0.000000        True      1767286799999   2026-01-01  BTCUSDT
4 2026-01-01 17:00:00  87988.77  87990.00  87988.77  87990.00   0.000000  1767286859999        0.000000      0        0.000000        0.000000        True      1767286859999   2026-01-01  BTCUSDT

[5 rows x 15 columns]

Symbols found:
        symbol
0  SOLUSDT
1  XRPUSDT
2  BTCUSDT
3  ETHUSDT

Checking for recent snapshots...
Recent snapshots found:
        symbol           timestamp        snapshot_time       close
0  SOLUSDT 2026-01-02 16:55:00  1767372959999    130.6600
1  XRPUSDT 2026-01-02 16:55:00  1767372959999      1.9583
2  BTCUSDT 2026-01-02 16:55:00  1767372959999  90212.9200
3  ETHUSDT 2026-01-02 16:55:00  1767372959999   3125.0700
4  SOLUSDT 2026-01-02 16:54:00  1767372899999    130.6300
```

---

### List All Data Files

```bash
python -c "import glob; print(glob.glob('binance-api/data/symbol=*/date=*/*.parquet'))"
```

**Example output (truncated):**

```
['binance-api/data\\symbol=BTCUSDT\\date=2026-01-01\\385c29136b024d13a77d4237595f0b40-0.parquet', ...]
```

---

### Inspect Raw Data in Parquet File

```bash
python -c "import pandas as pd; df = pd.read_parquet('binance-api/data/symbol=BTCUSDT/date=2026-01-01/385c29136b024d13a77d4237595f0b40-0.parquet'); print(df.head())"
```

**Example output:**

```
                        timestamp      open      high       low     close     volume        close_time  quote_asset_volume  trades  taker_buy_base  taker_buy_quote  is_closed     snapshot_time
0 2026-01-01 16:56:00  87972.06  87972.06  87972.06  87972.06   0.000000  1767286619999        0.000000      0        0.000000        0.000000        True      1767286619999
1 2026-01-01 16:57:00  87972.06  87972.06  87972.06  87972.06   0.000000  1767286679999        0.000000      0        0.000000        0.000000        True      1767286679999
2 2026-01-01 16:58:00  87972.06  87986.75  87972.06  87986.75   0.000000  1767286739999        0.000000      0        0.000000        0.000000        True      1767286739999
3 2026-01-01 16:59:00  87986.75  87988.77  87986.75  87988.77   0.000000  1767286799999        0.000000      0        0.000000        0.000000        True      1767286799999
4 2026-01-01 17:00:00  87988.77  87990.00  87988.77  87990.00   0.000000  1767286859999        0.000000      0        0.000000        0.000000        True      1767286859999
```