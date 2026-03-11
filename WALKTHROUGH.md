# BDP2 Pipeline Walkthrough

End-to-end guide for running the Big Data ML platform: from starting the infrastructure to shutting it down.

---

## Prerequisites
- Docker & Docker Compose installed.
- All commands are run from the **project root directory** (`BDP2/`).

---

## Step 1: Start the Server

Bring up all infrastructure services (Kafka, HDFS, Spark, MLflow):

```bash
docker-compose up -d
```

**Wait ~30 seconds**, then verify all containers are healthy:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Expected: `namenode`, `datanode`, `resourcemanager`, `spark-master`, `spark-worker`, `kafka`, `mlflow-server`, `mlflow-db` should all show `Up`.

Confirm HDFS has a live DataNode:

```bash
docker exec namenode hdfs dfsadmin -report
```

Expected: `Live datanodes (1)`.

---


## Step 2: Clean / Reset the Pipeline

> Skip this step on the first run. Use this to wipe all data and start fresh.

Run the reset script from your **local machine** (not inside a container):

```bash
python src/reset_pipeline.py
```

This will:
- Kill any running Spark/Python processes in containers.
- Delete all HDFS data at `/data/raw` and `/checkpoints/data_ingestion`.
- Re-create the Kafka `input_data` topic.
- Wipe the MLflow Postgres database and all saved model artifacts.

---

## Step 3: Generate Data

Starts streaming synthetic MNIST digit data (64 pixel features + label) to the Kafka `input_data` topic.

Run in a **dedicated terminal** (it runs continuously):

```bash
docker exec -it spark-master python3 /app/src/data_generator.py \
  --bootstrap-servers kafka:29092 \
  --topic input_data \
  --interval 0.5
```

**Arguments:**
| Argument | Default | Description |
| :--- | :--- | :--- |
| `--bootstrap-servers` | `kafka:29092` | Kafka broker address |
| `--topic` | `input_data` | Target Kafka topic |
| `--interval` | `1.0` | Seconds between messages |
| `--limit` | None | Stop after N messages (leave empty to run forever) |

You should see output like: `Starting MNIST data generation to topic 'input_data'...`

---

## Step 4: Ingest Data

Starts a **Spark Structured Streaming** job that reads from Kafka and writes Parquet files to HDFS.

Run in a **dedicated terminal** (it runs continuously):

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.driver.memory=512m \
  --conf spark.executor.memory=512m \
  /app/src/data_ingestion.py
```

To verify data is landing in HDFS, open a new terminal and run:

```bash
docker exec namenode hdfs dfs -ls -R /data/raw
```

You should see `.parquet` files being created.

---

## Step 5: Monitor the Pipeline

Tracks the number of messages in Kafka vs records persisted in HDFS and prints a sync report every 10 seconds.

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --conf spark.driver.memory=512m \
  --conf spark.executor.memory=512m \
  /app/src/monitor_pipeline.py
```

Example output:
```
[09:45:00]
  🔹 Kafka (Generated): 1,240 samples
  🔸 HDFS (Persisted):  1,105 samples
  ✅ Sync Progress:     89.1%
```

**Web UIs:**
| Dashboard | URL |
| :--- | :--- |
| HDFS NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| YARN Resource Manager | http://localhost:8088 |
| MLflow Experiment Tracker | http://localhost:5001 |

---

## Step 6: Train the Model

Trains a PyTorch neural network (MLP) on data in HDFS. It uses a **continual learning** approach: it reads the watermark of the last trained model and only trains on *new* data since that timestamp. It promotes the model to Production in MLflow **if it outperforms the current champion** (by >1% accuracy).

> **Trigger condition:** By default, training only runs if at least **500 new samples** have been ingested since the last training run. Use `--force` to override.

> ⚠️ **Memory Warning:** If you are running Docker Desktop with default 8GB memory, running PyTorch concurrently with `data_ingestion.py` and `monitor_pipeline.py` will likely cause an **Out Of Memory (Exit Code 137)** crash. Please `Ctrl+C` your ingestion and monitor streams before starting training!

**Run PyTorch Training:**
*(Note: Exceeding 512m for the Spark JVM may cause the container to OOM during PyTorch model logging)*
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.driver.memory=512m \
  --conf spark.executor.memory=512m \
  /app/src/pytorch_trainer.py
```

**Force training (bypass the 500 sample threshold):**
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf spark.driver.memory=512m \
  --conf spark.executor.memory=512m \
  /app/src/pytorch_trainer.py --force
```

**Arguments:**
| Argument | Default | Description |
| :--- | :--- | :--- |
| `--force` | False | Force training even if sample threshold not met |
| `--limit` | None | Cap number of training samples for this run |
| `--incremental` | False | Run in incremental mode |

After training, view results at **[http://localhost:5001](http://localhost:5001)**. The promoted model will be in the `Production` stage.

---

## Step 7: Shut Down the Server

### Stop (preserves data volumes)
```bash
docker-compose stop
```

### Stop and remove containers (data volumes are kept)
```bash
docker-compose down
```

### Full wipe (removes containers AND all data volumes)
> ⚠️ This is irreversible — all HDFS data, Kafka messages, and MLflow history will be deleted.
```bash
docker-compose down -v
```

---

## Troubleshooting

| Problem | Cause | Fix |
| :--- | :--- | :--- |
| `datanode` exits immediately | OOM | Reduce `HADOOP_HEAPSIZE_MAX` in `config/hadoop.env` (currently `512`) |
| Ingestion job fails with `addBlock` error | `datanode` is down | Restart services: `docker-compose up -d` |
| HDFS in Safe Mode | Fresh start / restart | Wait 30s or run: `docker exec namenode hdfs dfsadmin -safemode leave` |
