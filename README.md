# Crypto Options vs Rates

This repository contains Big Data project about integrating Polymarket prediction data and Binance cryptocurrency rates to analyse relationships between market expectations and real prices.

---

## Repository structure

### `data_ingestion/`
Contains all components responsible for **data acquisition** and **initial ingestion**:
- **`nifi/`** – NiFi templates implementing automated data flows (fetch, transform, push to Kafka).
- **`collectors/`** – Python scripts or connectors that pull raw data from external APIs
- **`kafka/`** – Scripts for Kafka topic creation, sample producers, or message utilities.

### `storage/`
Defines how **raw and processed data** are stored in the batch layer:
- **`hadoop/`** – Documentation of directory layout, partitioning, and storage conventions in HDFS.
- **`hive/`** – Hive DDL scripts and database/table definitions for structured batch storage.

### `batch_processing/`
Implements **batch processing** logic:
- **`spark_batch/`** – PySpark jobs for ETL, data cleaning, and generation of analytical batch views.

### `speed_processing/`
Implements **real-time (speed layer)** processing:
- **`spark/`** – Spark Structured Streaming jobs consuming from Kafka to compute live metrics or real-time views.

### `merge/`
Handles **reconciliation** between batch and speed layers:
- Scripts and documentation describing how real-time results are merged with historical batch data.

### `serving/`
Defines the **serving layer** that exposes processed data for querying:
- **`hbase/`** – Schema definitions and loaders for HBase store supporting random reads.
- API or utility scripts to query final, merged datasets.

### `console_scripts/`
Utility and **operational scripts**:
- Example commands for running collectors, Spark jobs, or data loading tasks.
- Batch execution and administrative scripts for the Hadoop ecosystem tools.

### `configs/`
Holds **template configuration files** (without secrets):
- Configuration samples for NiFi processors, Kafka, Spark, and HBase.

### `tests/`
Contains **functional test plans** and fixtures:
- **`functional_tests/`** – Markdown test cases describing goals, steps, expected results, and evidence placeholders.

### `report/`
Project documentation:
- **`assets/`** – Assets used in report.
- Final report.

### `requirements/`
Lists dependencies required for used technologies.

### Root files
- **`README.md`** – This file.
- **`.gitignore`** – Standard ignore list for local environment files.