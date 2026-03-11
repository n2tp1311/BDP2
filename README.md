# BDP2 – Big Data ML Platform

An end-to-end **Online Machine Learning pipeline** simulating a production data system using Hadoop, Kafka, Spark, and MLflow. Data flows from a streaming generator through HDFS into a continually trained PyTorch classifier, with built-in **throughput** and **latency** monitoring.

→ For step-by-step run instructions, see **[WALKTHROUGH.md](./WALKTHROUGH.md)**.

---

## Architecture

```
data_generator.py
 (MNIST → Kafka JSON)
        │
        ▼  Kafka topic: input_data
        │
data_ingestion.py
 (Spark Streaming → HDFS Parquet)
        │
        ▼  hdfs://namenode:9000/data/raw
        │
pytorch_trainer.py
 (Spark batch → MLP training → MLflow)
        │
        ▼
MLflow Model Registry
 (Champion / Challenger promotion)
```

---

## Components

| Service | Role |
|---------|------|
| **Kafka** | Streams MNIST feature messages from generator |
| **HDFS** | Stores Parquet files (features + metrics columns) |
| **YARN** | Resource manager for Spark jobs |
| **Spark Streaming** | Ingests from Kafka, computes latency, writes Parquet |
| **Spark Batch** | Reads HDFS, trains MLP, promotes best model |
| **PyTorch MLP** | `Input(64) → FC(32) → ReLU → FC(16) → ReLU → FC(10)` |
| **MLflow** | Experiment tracking + Model Registry |

---

## Parquet Schema (HDFS)

| Column | Type | Description |
|--------|------|-------------|
| `pixel_0` … `pixel_63` | Double | Flattened 8×8 MNIST pixel values |
| `label` | Integer | Digit class (0–9) |
| `produce_timestamp` | Double | Unix epoch when message was generated |
| `ingest_timestamp` | Timestamp | Spark processing time |
| `latency_ms` | Double | `(ingest_timestamp − produce_timestamp) × 1000` |

---

## Source Files

| File | Description |
|------|-------------|
| `src/data_generator.py` | Generates MNIST data → Kafka (includes `produce_timestamp`) |
| `src/data_ingestion.py` | Spark Streaming: Kafka → Parquet + computes `latency_ms` |
| `src/pytorch_trainer.py` | Continual learning trainer with champion/challenger promotion |
| `src/monitor_pipeline.py` | Live throughput (records/sec) and latency stats |
| `src/reset_pipeline.py` | Wipe HDFS, Kafka, MLflow to start fresh |
| `src/sql_analytics.py` | Spark SQL analytics on ingested data |

---

## Web UIs

| Dashboard | URL |
|-----------|-----|
| HDFS NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| YARN Resource Manager | http://localhost:8088 |
| MLflow | http://localhost:5001 |
| Kafka UI | http://localhost:8085 |
