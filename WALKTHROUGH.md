# BDP2 Pipeline Walkthrough

Step-by-step guide to run the full pipeline — from infrastructure startup to model promotion.

> All commands run from the **project root** (`BDP2/`) unless noted.  
> Prerequisites: Docker Desktop ≥ 8 GB RAM, Python 3.8+.

---

## Step 1 — Start Infrastructure

```bash
docker-compose up -d
```

Wait ~30s, then verify all containers are running:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Expected containers (`Up`): `namenode`, `datanode`, `resourcemanager`, `nodemanager`,
`spark-master`, `spark-worker`, `kafka`, `zookeeper`, `mlflow-server`, `mlflow-db`.

Confirm HDFS has a live DataNode:

```bash
docker exec namenode hdfs dfsadmin -report
# Expected: "Live datanodes (1)"
```

---

## Step 2 — Reset (Optional)

> Skip on first run. Use to wipe all data and start completely fresh.

```bash
python src/reset_pipeline.py   # run on local machine, NOT inside a container
```

Clears: HDFS `/data/raw`, Kafka `input_data` topic, MLflow database and artifacts.

---

## Step 3 — Install Dependencies in Spark Container

```bash
docker exec spark-master pip3 install -r /app/requirements.txt
```

---

## Step 4 — Generate Data (dedicated terminal)

Streams MNIST digit data to Kafka. Each message includes a `produce_timestamp` for latency tracking.

```bash
docker exec -it spark-master python3 /app/src/data_generator.py \
  --bootstrap-servers kafka:29092 \
  --topic input_data \
  --interval 0.5
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--bootstrap-servers` | `kafka:29092` | Kafka broker |
| `--topic` | `input_data` | Target topic |
| `--interval` | `1.0` | Seconds between messages |
| `--limit` | None | Stop after N messages (omit = infinite) |

---

## Step 5 — Ingest into HDFS (dedicated terminal)

Spark Structured Streaming reads from Kafka and writes Parquet to HDFS.  
Automatically computes `latency_ms = (ingest_timestamp − produce_timestamp) × 1000`.

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.driver.memory=512m \
  --conf spark.executor.memory=512m \
  /app/src/data_ingestion.py
```

Verify data is landing:

```bash
docker exec namenode hdfs dfs -ls /data/raw
```

---

## Step 6 — Monitor Throughput & Latency

Prints a live report every 10 seconds:

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --conf spark.driver.memory=512m \
  --conf spark.executor.memory=512m \
  /app/src/monitor_pipeline.py
```

Example output:
```
[14:21:00]
  🔹 Kafka  (Produced):  1,240 messages
  🔸 HDFS   (Persisted): 1,105 records
  ✅ Sync   Progress:    89.1%

  ⚡ Throughput:          2.30 records/sec  (+23 in last 10s)
  🕐 Latency (all):      3,412 ms avg
  🕑 Latency (last 30s): 2,187 ms avg
```

**Metric definitions:**

| Metric | Formula |
|--------|---------|
| Throughput | `Δ HDFS records / Δ time (10s interval)` |
| Latency (all) | `avg(latency_ms)` over all HDFS records |
| Latency (30s) | `avg(latency_ms)` for records with `produce_timestamp ≥ now − 30s` |

---

## Step 7 — Train the Model

> ⚠️ Stop ingestion and monitor jobs (Ctrl+C) before training to avoid OOM.

Reads HDFS data, trains a PyTorch MLP on data produced since the last watermark,
and promotes the challenger to Production if accuracy improves by > 1%.

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.driver.memory=512m \
  --conf spark.executor.memory=512m \
  /app/src/pytorch_trainer.py
```

Force training (bypass 500-sample threshold):
```bash
# append --force to the command above
/app/src/pytorch_trainer.py --force
```

| Argument | Default | Description |
|----------|---------|-------------|
| `--force` | False | Force even if < 500 new samples |
| `--limit` | None | Cap number of training samples |

View results at **http://localhost:5001** → `MyClassifier` model → Production stage.

---

## Step 8 — Shut Down

```bash
docker-compose stop          # pause (keeps data)
docker-compose down          # remove containers (keeps volumes)
docker-compose down -v       # ⚠️ full wipe including all volumes
```

---

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `datanode` exits immediately | OOM | Lower `HADOOP_HEAPSIZE_MAX` in `config/hadoop.env` |
| Ingestion fails (`addBlock`) | `datanode` down | `docker-compose up -d` |
| HDFS in Safe Mode | Fresh restart | `docker exec namenode hdfs dfsadmin -safemode leave` |
| Training OOM (exit 137) | Spark + PyTorch together | Stop ingestion/monitor before training |
| `latency_ms` shows `null` | Old data before `produce_timestamp` was added | Run `reset_pipeline.py` then restart |
