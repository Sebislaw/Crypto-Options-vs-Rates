# Crypto Options vs Rates

This repository contains a Big Data project about integrating Polymarket prediction data and Binance cryptocurrency rates to analyse relationships between market expectations and real prices.

---

## Repository structure

### `deployment/`
VM provisioning and operational notes for running services on local virtual machines or developer hosts. See `deployment/README.md` for provisioning and key usage.

### `ingestion_layer/`
Contains all components responsible for **data acquisition** and **initial ingestion**:
- **`binance/`** – Source code for the `ingestion-binance-collector` collector.
- **`polymarket/`** – Source code for the `ingestion-polymarket-collector` collector.
- **`nifi/`** – NiFi templates implementing automated data flows (fetch, transform, push to Kafka).

### `batch_layer/`
Defines how **raw and processed data** are stored and processed in the batch path:
- **`hadoop/`** – Configuration of directory layout, partitioning, and storage conventions in HDFS.
- **`hive/`** – Hive DDL scripts and database/table definitions for structured batch storage.
- **`spark/`** – PySpark jobs for ETL, data cleaning, and generation of analytical batch views.
- **`hbase/`** – HBase table creation and schema definitions for serving processed analytics.
- **Quick Start**: See `batch_layer/QUICKSTART.md` for execution guide.

### `speed_layer/`
Implements **real-time (speed layer)** buffering and processing:
- **`kafka/`** – Configuration for Kafka topic creation, sample producers, or message utilities.
- **`spark/`** – Spark Structured Streaming jobs consuming from Kafka to compute live metrics or real-time views.

### `serving_layer/`
Defines the layer that exposes processed data and handles **reconciliation**:
- **`hbase/`** – Schema definitions and loaders for HBase store supporting random reads.
- **`merge/`** – Logic describing how real-time results are merged with historical batch data.

### `console_scripts/`
Utility and **operational scripts**:
- **`utils/`** – Auxiliary operational utilities.
- Example commands for running collectors, Spark jobs, or data loading tasks.
- Batch execution and administrative scripts for the Hadoop ecosystem tools.

### `tests/`
Contains **functional test plans** and fixtures:
- **`functional_tests/`** – Markdown test cases describing goals, steps, expected results, and evidence placeholders.
- **`unit_tests/`** – Automated unit tests for individual code components.

### `report/`
Project documentation.

### `requirements/`
Lists dependencies required for used technologies.

---

## Directory Tree



```text
root:
├── .gitignore
├── README.md
├── deployment/
├── ingestion_layer/
│   ├── binance/
│   ├── polymarket/
│   └── nifi/
├── batch_layer/
│   ├── hadoop/
│   ├── hive/
│   └── spark/
├── speed_layer/
│   ├── kafka/
│   └── spark/
├── serving_layer/
│   ├── hbase/
│   └── merge/
├── console_scripts/
├── report/
├── requirements/
└── tests/
    ├── functional_tests/
    └── unit_tests/