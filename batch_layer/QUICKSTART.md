# Batch Layer - Quick Start Guide

This is a simplified guide to get your batch layer running quickly.

---

## 🚀 One-Command Execution

```bash
cd ~/Crypto-Options-vs-Rates
chmod +x batch_layer/run_batch_analytics.sh
batch_layer/run_batch_analytics.sh
```

That's it! The script handles everything automatically.

---

## 📋 Manual Step-by-Step (For Learning)

### 1. Start Services (if not running)

```bash
sudo /home/vagrant/scripts/bootstrap.sh
```

Wait 30 seconds for services to start, then verify:
```bash
hdfs dfsadmin -report  # Should show healthy
hive -e "SHOW DATABASES;"  # Should list databases
echo "status" | hbase shell  # Should show active
```

### 2. Create Hive Tables

```bash
cd ~/Crypto-Options-vs-Rates
hive -f batch_layer/hive/create_tables.sql
hive -f batch_layer/hive/repair_partitions.sql
```

Verify data exists:
```bash
hive -e "SELECT COUNT(*) FROM binance_klines;"
hive -e "SELECT COUNT(*) FROM polymarket_orderbook;"
```

### 3. Create HBase Table

```bash
chmod +x batch_layer/hbase/create_table.sh
bash batch_layer/hbase/create_table.sh
```

### 4. Run Spark Job

```bash
spark-submit \
    --master local[*] \
    --driver-memory 2g \
    batch_layer/spark/batch_analytics.py
```

### 5. View Results

```bash
# View in HBase
echo 'scan "market_analytics", {LIMIT => 5}' | hbase shell

# Or use the query tool
python batch_layer/spark/query_hbase.py
```

---

## 🔍 Common Issues

### "No data found!"
**Solution**: Make sure ingestion is running
```bash
# Check if data exists
hdfs dfs -ls /user/vagrant/cleansed/binance/
hdfs dfs -ls /user/vagrant/cleansed/polymarket_trade/

# If empty, start ingestion
console_scripts/start_ingestion.sh
```

### "HBase connection failed"
**Solution**: Start HBase Thrift server
```bash
# Check if running
ps aux | grep ThriftServer

# If not running, start it
hbase-daemon.sh start thrift

# Verify port 9090 is open
netstat -tulpn | grep 9090
```

### "Spark job fails"
**Solution**: Check logs
```bash
cat ~/Crypto-Options-vs-Rates/logs/spark_batch_analytics.log
```

---

## 📊 What to Expect

### Successful Output
```
============================================================
  Crypto Market Batch Analytics - Starting
============================================================

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
|UNCERTAIN        |41   |
+-----------------+-----+
```

---

## 🎯 Key Metrics Explained

- **CORRECT_BULL**: Market predicted UP (prob > 0.6) and price went up ✅
- **FAILED_BULL**: Market predicted UP but price went down ❌
- **CORRECT_BEAR**: Market predicted DOWN and price went down ✅
- **FAILED_BEAR**: Market predicted DOWN but price went up ❌
- **UNCERTAIN**: Prediction confidence was too low (prob 0.4-0.6)

---

## 🛠️ Useful Commands

### Check Hive Data
```bash
hive -e "SELECT symbol, COUNT(*) FROM binance_klines GROUP BY symbol;"
hive -e "SELECT crypto, COUNT(*) FROM polymarket_orderbook GROUP BY crypto;"
```

### Check HBase Data
```bash
# Count rows
echo "count 'market_analytics'" | hbase shell

# View specific symbol
echo 'scan "market_analytics", {STARTROW => "BTCUSDT", LIMIT => 10}' | hbase shell

# Delete table (fresh start)
echo "disable 'market_analytics'; drop 'market_analytics'" | hbase shell
```

### Re-run for Specific Date
```bash
batch_layer/run_batch_analytics.sh 2026-01-11
```

---

## 📁 Files Created

```
batch_layer/
├── README.md                      ← Full documentation
├── QUICKSTART.md                  ← This file
├── run_batch_analytics.sh         ← Main execution script
├── hive/
│   ├── create_tables.sql          ← Hive DDL
│   └── repair_partitions.sql      ← Partition discovery
├── spark/
│   ├── batch_analytics.py         ← Main PySpark job
│   ├── config.py                  ← Configuration
│   └── query_hbase.py             ← HBase query tool
└── hbase/
    └── create_table.sh            ← HBase setup
```

---

## ✅ Checklist for Demo/Testing

- [ ] Services running (`bootstrap.sh`)
- [ ] Hive tables created and populated
- [ ] HBase table created
- [ ] Spark job runs without errors
- [ ] Results visible in HBase
- [ ] Can query and see prediction accuracy stats

---

## 🆘 Need Help?

1. Check logs: `ls -lh ~/Crypto-Options-vs-Rates/logs/`
2. View detailed README: `cat batch_layer/README.md`
3. Verify data flow: `hdfs dfs -ls -R /user/vagrant/cleansed/`

---

**Good luck with your implementation!** 🚀
