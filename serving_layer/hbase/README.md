# HBase Serving Layer

This module provides the HBase infrastructure for the Lambda Architecture serving layer.

## Contents

| File | Description |
|------|-------------|
| `setup_hbase.sh` | Automated table creation script |
| `hbase_schema.hbase` | HBase shell schema definition |
| `config.py` | Python constants and RowKey utilities |
| `hbase_writer.py` | Standalone HBase writer (HappyBase) |
| `spark_hbase_connector.py` | PySpark integration for batch/streaming writes |

## Tables

### `market_analytics` (Batch Layer)

Stores 15-minute window aggregations from batch processing.

- **RowKey**: `SYMBOL#WINDOW_END_TIMESTAMP` (e.g., `BTC#1767201300`)
- **Column Families**:
  - `prices`: open, close, high, low, volume, volatility
  - `betting`: avg_prob_up, avg_prob_down, total_bet_vol, sentiment_score
  - `correlation`: prediction_result, divergence

### `market_live` (Speed Layer)

Stores real-time streaming data points for dashboard display.

- **RowKey**: `SYMBOL#REVERSE_TIMESTAMP` (newest sorts first)
- **Column Family**:
  - `d`: binance_price, poly_last_trade, poly_best_bid, poly_best_ask, implied_prob

## Usage

### Setup (included in initialize_project.sh)

```bash
bash serving_layer/hbase/setup_hbase.sh
```

### Test HBase Connectivity

```bash
python serving_layer/hbase/hbase_writer.py --test
```

### Write Data (Python)

```python
from serving_layer.hbase.hbase_writer import HBaseWriter

with HBaseWriter() as writer:
    writer.write_batch_analytics(
        symbol='BTC',
        window_end_ts=1767201300,
        prices_data={'close': 92500.5, 'volatility': 0.02},
        betting_data={'avg_prob_up': 0.65},
        correlation_data={'prediction_result': 'CORRECT'}
    )
```

### Write Data (Spark)

```python
from serving_layer.hbase.spark_hbase_connector import write_batch_to_hbase

write_batch_to_hbase(df)  # Spark DataFrame with expected schema
```
