# Batch Layer Troubleshooting Guide

This guide covers common issues and their solutions.

---

## 🔍 Quick Diagnostics

Run the verification script first:
```bash
cd ~/Crypto-Options-vs-Rates
bash batch_layer/verify_environment.sh
```

---

## 🚨 Common Issues

### 1. "No data found in Hive tables"

**Symptoms:**
```
[ERROR] No data found! Make sure:
  1. Tables are created
  2. Partitions are repaired
  3. Data exists in HDFS
```

**Diagnosis:**
```bash
# Check if data exists in HDFS
hdfs dfs -ls /user/vagrant/cleansed/binance/
hdfs dfs -ls /user/vagrant/cleansed/polymarket_trade/

# Check if Hive tables exist
hive -e "SHOW TABLES;"

# Check row counts
hive -e "SELECT COUNT(*) FROM binance_klines;"
hive -e "SELECT COUNT(*) FROM polymarket_orderbook;"
```

**Solutions:**

**A. If HDFS directories are empty:**
```bash
# Start ingestion to populate data
cd ~/Crypto-Options-vs-Rates
console_scripts/start_ingestion.sh

# Wait a few minutes, then check
hdfs dfs -ls -R /user/vagrant/cleansed/
```

**B. If tables don't exist:**
```bash
# Create tables
hive -f batch_layer/hive/create_tables.sql

# Discover partitions
hive -f batch_layer/hive/repair_partitions.sql
```

**C. If tables exist but show 0 rows:**
```bash
# Repair partitions to discover data
hive -e "MSCK REPAIR TABLE binance_klines;"
hive -e "MSCK REPAIR TABLE polymarket_orderbook;"

# Verify partitions were added
hive -e "SHOW PARTITIONS binance_klines;"
```

---

### 2. "HBase connection failed"

**Symptoms:**
```
[ERROR] Failed to connect to HBase
ConnectionError: [Errno 111] Connection refused
```

**Diagnosis:**
```bash
# Check if HBase is running
echo "status" | hbase shell

# Check if Thrift server is running
ps aux | grep ThriftServer

# Check port 9090
netstat -tulpn | grep 9090
```

**Solutions:**

**A. HBase not running:**
```bash
# Start HBase
sudo /home/vagrant/scripts/bootstrap.sh

# Or start manually
start-hbase.sh
```

**B. Thrift server not running:**
```bash
# Start Thrift server
hbase-daemon.sh start thrift

# Verify it started
sleep 5
ps aux | grep ThriftServer

# Check logs if it failed
tail -50 /usr/local/hbase/logs/hbase-*-thrift-*.log
```

**C. Port conflict:**
```bash
# Find what's using port 9090
sudo lsof -i :9090

# Kill the process if needed
sudo kill -9 <PID>

# Restart Thrift
hbase-daemon.sh stop thrift
hbase-daemon.sh start thrift
```

---

### 3. "Empty join results"

**Symptoms:**
```
[WARNING] No matching windows found between Binance and Polymarket!
  This might mean the timestamps don't overlap.
```

**Diagnosis:**
```bash
# Check date ranges in each table
hive -e "SELECT MIN(date), MAX(date) FROM binance_klines;"
hive -e "SELECT MIN(date), MAX(date) FROM polymarket_orderbook;"

# Check if both have same dates
hive -e "SELECT DISTINCT date FROM binance_klines ORDER BY date;"
hive -e "SELECT DISTINCT date FROM polymarket_orderbook ORDER BY date;"

# Check row counts by date
hive -e "SELECT date, COUNT(*) FROM binance_klines GROUP BY date;"
hive -e "SELECT date, COUNT(*) FROM polymarket_orderbook GROUP BY date;"
```

**Solutions:**

**A. Different date ranges:**
```bash
# Run batch job for specific overlapping date
batch_layer/run_batch_analytics.sh 2026-01-11  # Use a date that exists in both
```

**B. Wait for more data:**
```bash
# Make sure ingestion is running
ps aux | grep "python.*main.py\|polymarket_clob"

# If not running, start it
console_scripts/start_ingestion.sh

# Wait 15-30 minutes for data to accumulate
# NiFi processes files periodically
```

**C. Check symbol mapping:**
```bash
# See what symbols/markets exist
hive -e "SELECT DISTINCT symbol FROM binance_klines;"
hive -e "SELECT DISTINCT market FROM polymarket_orderbook LIMIT 20;"

# Make sure symbols match expected: btc, eth, sol, xrp
```

---

### 4. "HBase write failed"

**Symptoms:**
```
[!] Error writing to HBase: <error message>
Saving to Parquet backup instead...
```

**Diagnosis:**
```bash
# Check if happybase is installed
python -c "import happybase; print('OK')"

# Check HBase table exists
echo "exists 'market_analytics'" | hbase shell

# Check HBase logs
tail -50 /usr/local/hbase/logs/hbase-*-master-*.log
```

**Solutions:**

**A. happybase not installed:**
```bash
pip install happybase

# Verify installation
python -c "import happybase; print(happybase.__version__)"
```

**B. Table doesn't exist:**
```bash
# Create the table
bash batch_layer/hbase/create_table.sh

# Verify
echo "describe 'market_analytics'" | hbase shell
```

**C. Use backup Parquet output:**
```bash
# Check where backup was saved
hdfs dfs -ls /user/vagrant/batch/

# Read the backup
spark-shell --master local[*]
scala> val df = spark.read.parquet("/user/vagrant/batch/analytics_results")
scala> df.show(10)
```

---

### 5. "Spark job fails with OOM"

**Symptoms:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Solutions:**

**Increase memory:**
```bash
spark-submit \
    --master local[*] \
    --driver-memory 4g \
    --executor-memory 4g \
    --conf spark.driver.maxResultSize=2g \
    batch_layer/spark/batch_analytics.py
```

**Process smaller date range:**
```bash
# Process one day at a time
batch_layer/run_batch_analytics.sh 2026-01-11
```

---

### 6. "Hive Metastore not accessible"

**Symptoms:**
```
MetaException: Could not connect to meta store
```

**Solutions:**

```bash
# Check if Hive Metastore is running
ps aux | grep HiveMetaStore

# Check port 9083
netstat -tulpn | grep 9083

# Restart Hive Metastore
sudo /home/vagrant/scripts/bootstrap.sh

# Or manually
nohup hive --service metastore &
```

---

### 7. "Permission denied in HDFS"

**Symptoms:**
```
Permission denied: user=<user>, access=WRITE
```

**Solutions:**

```bash
# Fix permissions
hdfs dfs -chmod -R 775 /user/vagrant/

# Change ownership
hdfs dfs -chown -R vagrant:vagrant /user/vagrant/

# Verify
hdfs dfs -ls -R /user/vagrant/ | head -20
```

---

### 8. "Services not starting"

**Symptoms:**
```
[ERROR] HDFS/Hive/HBase is not accessible
```

**Solutions:**

**Full service restart:**
```bash
# Stop all services
stop-all.sh
stop-hbase.sh

# Start all services
sudo /home/vagrant/scripts/bootstrap.sh

# Wait 30 seconds
sleep 30

# Verify each service
hdfs dfsadmin -report
hive -e "SHOW DATABASES;"
echo "status" | hbase shell
```

**Check logs:**
```bash
# HDFS logs
tail -50 /usr/local/hadoop/logs/hadoop-*-namenode-*.log

# HBase logs
tail -50 /usr/local/hbase/logs/hbase-*-master-*.log

# Check for port conflicts
sudo netstat -tulpn | grep -E '(9000|9083|16010|50070)'
```

---

## 🔧 Debugging Tips

### Enable Verbose Logging

**Spark:**
```bash
export SPARK_SUBMIT_OPTS="-Dlog4j.configuration=file:log4j.properties"
spark-submit ... batch_layer/spark/batch_analytics.py
```

**Hive:**
```bash
hive --hiveconf hive.root.logger=DEBUG,console -f batch_layer/hive/create_tables.sql
```

### Check Spark UI

While job is running, open:
```
http://localhost:4040
```

### Manual Data Inspection

**Check Parquet schema:**
```bash
spark-shell --master local[*]

scala> val df = spark.read.parquet("/user/vagrant/cleansed/binance")
scala> df.printSchema()
scala> df.show(5, truncate=false)
```

**Check HBase data:**
```bash
# Scan table
echo 'scan "market_analytics", {LIMIT => 10}' | hbase shell

# Count rows
echo 'count "market_analytics"' | hbase shell

# Get specific row
echo 'get "market_analytics", "BTCUSDT_9999999998232799600"' | hbase shell
```

---

## 📋 Pre-Flight Checklist

Before running the batch job, verify:

- [ ] Services running: `ps aux | grep -E '(NameNode|HMaster|HiveMetaStore)'`
- [ ] HDFS healthy: `hdfs dfsadmin -report`
- [ ] Data in HDFS: `hdfs dfs -ls /user/vagrant/cleansed/binance/`
- [ ] Hive tables exist: `hive -e "SHOW TABLES;"`
- [ ] Hive has data: `hive -e "SELECT COUNT(*) FROM binance_klines;"`
- [ ] HBase running: `echo "status" | hbase shell`
- [ ] Thrift running: `ps aux | grep ThriftServer`
- [ ] Python deps: `python -c "import happybase"`
- [ ] Scripts executable: `ls -lh batch_layer/*.sh`

---

## 🆘 Nuclear Option (Fresh Start)

If all else fails:

```bash
# 1. Stop everything
stop-all.sh
stop-hbase.sh

# 2. Clean old data (CAUTION: destroys data)
rm -rf /tmp/hadoop-*
rm -rf /tmp/hbase-*

# 3. Restart services
sudo /home/vagrant/scripts/bootstrap.sh

# 4. Wait for services
sleep 60

# 5. Recreate everything
cd ~/Crypto-Options-vs-Rates

# Restart ingestion
console_scripts/start_ingestion.sh

# Wait for data (15 minutes)
sleep 900

# Run batch setup
hive -f batch_layer/hive/create_tables.sql
hive -f batch_layer/hive/repair_partitions.sql
bash batch_layer/hbase/create_table.sh

# Try batch job
batch_layer/run_batch_analytics.sh
```

---

## 📞 Getting Help

**Check logs:**
```bash
ls -lh ~/Crypto-Options-vs-Rates/logs/
cat ~/Crypto-Options-vs-Rates/logs/spark_batch_analytics.log
```

**System health:**
```bash
# Disk space
df -h

# Memory
free -h

# Services
jps
```

**Manual testing:**
```bash
# Test Spark locally
pyspark --master local[*]
>>> spark.sql("SELECT COUNT(*) FROM binance_klines").show()

# Test HBase
python batch_layer/spark/query_hbase.py
```

---

Good luck! Most issues can be solved by restarting services and verifying data exists. 🚀
