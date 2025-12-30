# Crypto Options vs Rates

This repository contains a Big Data project about integrating Polymarket prediction data and Binance cryptocurrency rates to analyse relationships between market expectations and real prices.

---

## Repository structure

### `deployment/`
Orchestration and build hub for the system:
- **`docker-compose.yml`** – Definition of all **14 containers**, including networks and volumes.
- **`binance-collector/`** – Dockerfile for `ingestion-binance-collector`.
- **`polymarket-collector/`** – Dockerfile for `ingestion-polymarket-collector`.
- **`spark-base/`** – Shared Dockerfile for all Spark-related processing units.

### `ingestion_layer/`
Contains all components responsible for **data acquisition** and **initial ingestion**:
- **`binance/`** – Source code for the `ingestion-binance-collector` container.
- **`polymarket/`** – Source code for the `ingestion-polymarket-collector` container.
- **`nifi/`** – NiFi templates implementing automated data flows (fetch, transform, push to Kafka).

### `batch_layer/`
Defines how **raw and processed data** are stored and processed in the batch path:
- **`hadoop/`** – Configuration of directory layout, partitioning, and storage conventions in HDFS.
- **`hive/`** – Hive DDL scripts and database/table definitions for structured batch storage.
- **`spark/`** – PySpark jobs for ETL, data cleaning, and generation of analytical batch views.

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

### `configs/`
Holds **template configuration files** (without secrets):
- Configuration samples for NiFi processors, Kafka, Spark, and HBase.

### `tests/`
Contains **functional test plans** and fixtures:
- **`functional_tests/`** – Markdown test cases describing goals, steps, expected results, and evidence placeholders.
- **`unit_tests/`** – Automated unit tests for individual code components.

### `report/`
Project documentation:
- **`assets/`** – Assets used in report.
- Final report.

### `requirements/`
Lists dependencies required for used technologies.

---

## Directory Tree



```text
C:.
├── .gitignore                  # Git exclusion rules
├── README.md                   # Project overview
├── deployment/                 # Docker orchestration and build instructions
├── ingestion_layer/            # INGESTION LAYER
│   ├── binance/                # Serves: ingestion-binance-collector
│   ├── polymarket/             # Serves: ingestion-polymarket-collector
│   └── nifi/                   # Serves: ingestion-nifi
├── batch_layer/                # BATCH LAYER
│   ├── hadoop/                 # Serves: batch-hdfs-namenode, batch-hdfs-datanode
│   ├── hive/                   # Serves: batch-hive-metastore, batch-hive-server
│   └── spark/                  # Serves: batch-spark-worker
├── speed_layer/                # SPEED LAYER
│   ├── kafka/                  # Serves: speed-kafka
│   └── spark/                  # Serves: speed-spark-worker
├── serving_layer/              # SERVING LAYER
│   ├── hbase/                  # Serves: serving-hbase-master
│   └── merge/                  # Serves: serving-hbase-regionserver
├── configs/                    # Serves: shared-spark-master, shared-zookeeper
├── console_scripts/            # Management and operational tools
│   └── utils/
├── report/                     # Project documentation
│   └── assets/
├── requirements/               # Global dependencies
└── tests/                      # Testing suites
    ├── functional_tests/
    └── unit_tests/