# Big Data ML MVP

This project simulates an end-to-end Online Machine Learning pipeline using Hadoop, Kafka, Spark, and MLflow.

## Architecture

- **Data Source**: A Python script (`src/data_generator.py`) generates mock user events and sends them to Kafka.
- **Ingestion**: A Spark Structured Streaming job (`src/data_ingestion.py`) reads from Kafka and writes raw data to HDFS in Parquet format.
- **Training**: A Spark batch job (`src/model_trainer.py`) trains a Challenger model on the accumulated data, evaluates it against the current Champion model (stored in MLflow), and promotes it if it performs better.
- **Storage**: HDFS is used for data storage.
- **Orchestration & Resource Management**: YARN manages resources for Spark jobs.
- **Tracking**: MLflow tracks experiments and manages the model registry.

## Directory Structure

```
├── config/             # Configuration files (e.g., hadoop.env)
├── docker/             # Dockerfiles
├── src/                # Source code
│   ├── data_generator.py
│   ├── data_ingestion.py
│   └── model_trainer.py
├── docker-compose.yml  # Docker services definition
├── requirements.txt    # Python dependencies
└── README.md           # Project documentation
```

## Setup & Usage

### Prerequisites
- Docker and Docker Compose installed.

### 1. Start Infrastructure
```bash
docker-compose up -d --build
```
This will start Zookeeper, Kafka, HDFS (Namenode, Datanode), YARN (ResourceManager, NodeManager), Spark (Master, Worker), MLflow (Server, DB), and HistoryServer.

### 2. Install Dependencies
You can install the Python dependencies locally to run scripts or for development (though scripts are designed to run in the Spark container).
```bash
pip install -r requirements.txt
```

### 3. Run the Pipeline

**Step 1: Install dependencies in Spark container**
```bash
docker exec spark-master pip install -r /app/requirements.txt
```

**Step 2: Start Data Generator**
Run this in a separate terminal (locally or inside a container).
```bash
# Locally (requires pip install -r requirements.txt)
python src/data_generator.py --bootstrap-servers localhost:9092

# Or inside Spark Master container
docker exec -it spark-master python /app/src/data_generator.py --bootstrap-servers kafka:29092
```

**Step 3: Submit Data Ingestion Job**
Submits the streaming job to Spark.
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /app/src/data_ingestion.py
```

**Step 4: Train & Evaluate Model**
Run this periodically to train a new model on fresh data.
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /app/src/model_trainer.py
```

## Access UIs

- **HDFS**: http://localhost:9870
- **YARN**: http://localhost:8088
- **Spark Master**: http://localhost:8080
- **MLflow**: http://localhost:5001
- **Kafka UI**: http://localhost:8085
