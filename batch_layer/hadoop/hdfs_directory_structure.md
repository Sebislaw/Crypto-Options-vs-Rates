# HDFS Directory Structure for Batch Layer

## Overview

The batch layer uses HDFS to store both raw and cleansed data, following a partitioned structure for efficient processing.

---

## Directory Layout

```
/user/vagrant/
├── raw/                          # Raw data from ingestion layer
│   ├── binance/
│   │   ├── symbol=BTCUSDT/
│   │   │   └── date=2026-01-11/
│   │   │       └── *.parquet
│   │   ├── symbol=ETHUSDT/
│   │   ├── symbol=SOLUSDT/
│   │   └── symbol=XRPUSDT/
│   ├── polymarket_trade/
│   │   └── *.json              # JSON lines from WebSocket
│   └── polymarket_metadata/
│       └── date=2026-01-11/
│           └── *.json
│
├── cleansed/                     # Processed by NiFi (unified Parquet)
│   ├── binance/
│   │   └── date=2026-01-11/
│   │       └── *.parquet        # Used by Hive
│   ├── polymarket_trade/
│   │   └── date=2026-01-11/
│   │       └── *.parquet        # Used by Hive
│   └── polymarket_metadata/
│       └── date=2026-01-11/
│           └── *.parquet
│
└── batch/                        # Batch layer outputs
    └── analytics_results/        # Backup/debugging (optional)
        └── *.parquet
```

---

## Partitioning Strategy

### Binance Data
- **Raw**: Partitioned by `symbol` and `date`
- **Cleansed**: Partitioned by `date` only
- **Format**: Parquet with SNAPPY compression

### Polymarket Data
- **Raw**: Flat structure with date-based filenames
- **Cleansed**: Partitioned by `date`
- **Format**: Parquet with SNAPPY compression

---

## HDFS Commands Reference

### Check Directory Structure
```bash
hdfs dfs -ls -R /user/vagrant/cleansed/
```

### Check Data Size
```bash
hdfs dfs -du -h /user/vagrant/cleansed/binance/
hdfs dfs -du -h /user/vagrant/cleansed/polymarket_trade/
```

### View Sample Data
```bash
# Using hdfs cat (for text-based formats)
hdfs dfs -cat /user/vagrant/cleansed/binance/date=2026-01-11/*.parquet | head

# Better: Use Spark or Hive to read Parquet
spark-shell --master local[*]
scala> spark.read.parquet("/user/vagrant/cleansed/binance").show(5)
```

### Create Directories (if needed)
```bash
hdfs dfs -mkdir -p /user/vagrant/batch/analytics_results
hdfs dfs -chmod -R 755 /user/vagrant/batch
```

---

## Storage Considerations

- **Retention**: Keep at least 30 days of cleansed data
- **Compression**: SNAPPY balances speed and size
- **Replication**: Default HDFS replication factor (usually 1 for local, 3 for cluster)

---

## Maintenance

### Clean Old Partitions
```bash
# Remove data older than 30 days
hdfs dfs -rm -r /user/vagrant/cleansed/binance/date=2025-12-*
```

### Check HDFS Health
```bash
hdfs dfsadmin -report
hdfs fsck /user/vagrant/cleansed -files -blocks -locations
```
