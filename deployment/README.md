# Virtual Machine Environment Setup

This project uses a pre-configured Virtual Machine (Ubuntu via VirtualBox) provided for the course. This document outlines how to connect to the VM and configure your local development environment.

## 1. Prerequisites

* **Oracle VirtualBox** installed.

* The course **`.ova` image** imported into VirtualBox.

* The VM status must be **Running**.

## 2. Port Forwarding Configuration

Configure the following rules in VirtualBox (**Settings -> Network -> Adapter 1 -> Advanced -> Port Forwarding**).

| Name | Protocol | Host IP | Host Port | Guest IP | Guest Port |
| :--- | :--- | :--- | :--- | :--- | :--- |
| ssh | TCP | 127.0.0.1 | 2222 | | |
| tcp16010 | TCP | | 16010 | | 16010 |
| tcp18080 | TCP | | 18080 | | 18080 |
| tcp18888 | TCP | | 18888 | | 18888 |
| tcp4040 | TCP | | 4040 | | 4040 |
| tcp50070 | TCP | | 50070 | | 50070 |
| tcp50075 | TCP | | 50075 | | 50075 |
| tcp8080 | TCP | | 8080 | | 8080 |
| tcp8081 | TCP | | 8081 | | 8081 |
| tcp8088 | TCP | | 8088 | | 8088 |
| tcp9083 | TCP | | 9083 | | 9083 |
| tcp9443 | TCP | | 9443 | | 9443 |

## 3. SSH Connection (Terminal)

The VM exposes the SSH service on the host's loopback interface via port **2222**.

To connect, you need the **private key**, available from labs.

### Connection Command

**Syntax:**

```bash

ssh -i "<PATH\_TO\_PRIVATE\_KEY>" vagrant@localhost -p 2222

```

## 4. Inside the VM:

### Shutting down

When shutting down, do not force shutdown (power button icon in VirtualBox). Turn off, by sending shutdown sygnal to the VM (bolt icon in VirtualBox).

The preferred method of shutting down is using the VirtualBox `Save the machine state` option. This saves the current state, without turning anything off.

You can also shutdown via:

```bash

sudo shutdown -h now

```

Make sure the pipeline is stopped via `console_scripts/stop_ingestion.sh` before shutting down.


### When resuming the VM

For resuming the VM without errors, there are 2 approaches, with the first one being preferred:

1. Use the VirtualBox `Save the machine state` option. This saves the current state, without turning anything off. If the system worked before, chances are, now it will too.

2. Do a proper shutdown. This closes some of the services and causes problems with zombie processes in NiFi. To make the system work again, you have to manually execute the steps below. Starting the NiFi again will cause it to fetch files from hdfs again, which can take some time. The steps to take:

- Manually delete all elements from `http://localhost:9443/nifi/`.

- Run `initialize_project.sh`.

### When setting up a new VM

```bash

git clone https://github.com/Sebislaw/Crypto-Options-vs-Rates.git

cd Crypto-Options-vs-Rates

chmod +x console_scripts/initialize_project.sh

sed -i 's/\r$//' console_scripts/*.sh

console_scripts/initialize_project.sh

```

### Running the system

To start ingestion phase:

```bash

console_scripts/start_ingestion.sh

```

To stop ingestion phase:

```bash

console_scripts/stop_ingestion.sh

```

## 5. Data flow.

### Ingestion layer:

The python collectors save the output to hdfs in raw/ directory.

NiFi fetches from raw/ directory and coverts all inputs to unifies parquet format, then saves in hdfs in cleansed/ directory. NiFi also streams data to three Kafka topics: `binance`, `polymarket_trade` and `polymarket_metadata`.

To view each Kafka topic stream data:

```bash
/usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic binance --from-beginning

/usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic polymarket_trade --from-beginning

/usr/local/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic polymarket_metadata --from-beginning
```

### Speed layer:

The first part of the speed layer is Kafka, which partly is implemented in the ingestion layer in `NiFI_Flow.xml`. Kafka serves as an ingestion phase component dedicated for the speed layer.

Three Kafka topics are created: `binance`, `polymarket_trade` and `polymarket_metadata`. The first 2 are streamed into Spark in the speed layer. The metadata topic is currently not used, as saving historical data (15 minute markets, so 15 minutes at least) in the speed layer is not a functional choice beacuse of the virtual machines we use for development. The metadata from batch layer will be used in the serving layer to correctly map the data in from the speed layer.

Spark implementation for the speed layer is located in `speed_layer/spark` directory. Script `console_scripts/initialize_project.sh` sets up the spark job and the data from spark is saved directly in the HBASE.

To see spark batches: 

```bash
tail -f ~/Crypto-Options-vs-Rates/logs/spark_speed_layer.log
```