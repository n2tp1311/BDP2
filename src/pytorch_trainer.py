import os
import gc
import argparse
import mlflow
import mlflow.pytorch
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd

# Suppress noisy git warning from gitpython used inside mlflow
os.environ.setdefault("GIT_PYTHON_REFRESH", "quiet")

# Constants
SAMPLE_TRIGGER_THRESHOLD = 500
PROMOTION_ACCURACY_THRESHOLD = 0.01
MODEL_NAME = "MyClassifier"

class MNIST_MLP(nn.Module):
    """Simple MLP for 8x8 (64 pixels) MNIST digit classification."""
    def __init__(self):
        super(MNIST_MLP, self).__init__()
        self.fc1 = nn.Linear(64, 32)
        self.fc2 = nn.Linear(32, 16)
        self.fc3 = nn.Linear(16, 10)
        self.relu = nn.ReLU()
        
    def forward(self, x):
        x = self.relu(self.fc1(x))
        x = self.relu(self.fc2(x))
        x = self.fc3(x)
        return x

def get_last_run_metadata(client):
    """Retrieve internal accuracy of current Production model."""
    try:
        latest_versions = client.get_latest_versions(MODEL_NAME, stages=["Production"])
        if latest_versions:
            prod_version = latest_versions[0]
            run = client.get_run(prod_version.run_id)
            last_accuracy = run.data.metrics.get("accuracy", 0.0)
            return prod_version, last_accuracy
    except Exception as e:
        print(f"No existing production model found: {e}")
    return None, 0.0

def train_model(train_loader, epochs=15, initial_model=None):
    """Standard PyTorch training loop. Fine-tunes initial_model if provided."""
    model = initial_model if initial_model is not None else MNIST_MLP()
    optimizer = optim.Adam(model.parameters(), lr=0.005)
    criterion = nn.CrossEntropyLoss()
    
    model.train()
    print(f"Starting Training for {epochs} epochs...")
    for epoch in range(epochs):
        running_loss = 0.0
        for data, target in train_loader:
            optimizer.zero_grad()
            output = model(data)
            loss = criterion(output, target)
            loss.backward()
            optimizer.step()
            running_loss += loss.item()
        
        if (epoch + 1) % 5 == 0:
            print(f"Epoch {epoch+1}, Loss: {running_loss/len(train_loader):.4f}")
            
    return model

def evaluate_model(model, test_loader):
    """Calculate accuracy on test set."""
    model.eval()
    correct = 0
    total = 0
    with torch.no_grad():
        for data, target in test_loader:
            output = model(data)
            _, predicted = torch.max(output.data, 1)
            total += target.size(0)
            correct += (predicted == target).sum().item()
    return correct / total

def main(limit=None, force=False, incremental=False):
    # Initialize Spark for data loading
    spark = SparkSession.builder.appName("PyTorch-Trainer-MNIST").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # MLflow Setup
    mlflow.set_tracking_uri("http://mlflow-server:5001")
    mlflow.set_experiment("champion_challenger_experiment")
    client = mlflow.tracking.MlflowClient()
    
    # 1. Champion/Production Metadata and Watermark
    champion_version, champion_accuracy = get_last_run_metadata(client)
    
    last_timestamp = "1970-01-01 00:00:00"
    model = MNIST_MLP()
    
    if champion_version:
        print(f"Loading Production Model Version {champion_version.version} (Acc: {champion_accuracy:.4f})")
        model_uri = f"models:/{MODEL_NAME}/Production"
        try:
            model = mlflow.pytorch.load_model(model_uri)
            champion_run = client.get_run(champion_version.run_id)
            last_timestamp = champion_run.data.tags.get("last_timestamp", "1970-01-01 00:00:00")
            print(f"Continual Learning: Resuming from watermark {last_timestamp}")
        except Exception as e:
            print(f"Could not load production model weights, starting fresh: {e}")

    # 2. Load Data from HDFS
    data_path = "hdfs://namenode:9000/data/raw"
    try:
        from pyspark.sql.functions import col, max as spark_max
        df = spark.read.parquet(data_path).dropna()
        
        # 3. Filter for NEW data
        new_data_df = df.filter(col("ingest_timestamp") > last_timestamp)
        new_count = new_data_df.count()
        
        if new_count == 0:
            print(f"No new data found since {last_timestamp}. Exiting.")
            return

        print(f"Processing {new_count} new samples...")
        full_pdf = new_data_df.limit(3000).toPandas()
        
        # Capture the latest timestamp from the data we're about to train on
        new_watermark = str(full_pdf["ingest_timestamp"].max())
        spark.stop()
    except Exception as e:
        print(f"Failed to process data: {e}")
        return

    feature_cols = [f"pixel_{i}" for i in range(64)]
    
    with mlflow.start_run(run_name="PyTorch-Continual-Learning") as run:
        mlflow.log_param("framework", "pytorch")
        mlflow.set_tag("resume_from", last_timestamp)
        
        # Bridge to PyTorch
        X = torch.FloatTensor(full_pdf[feature_cols].values)
        y = torch.LongTensor(full_pdf["label"].values)
        
        dataset = TensorDataset(X, y)
        train_size = int(0.8 * len(dataset))
        test_size = len(dataset) - train_size
        train_ds, test_ds = torch.utils.data.random_split(dataset, [train_size, test_size])
        
        train_loader = DataLoader(train_ds, batch_size=32, shuffle=True)
        test_loader = DataLoader(test_ds, batch_size=32)
        
        # Fine-tune the loaded champion model (or train fresh if none exists)
        model = train_model(train_loader, epochs=5, initial_model=model)
        
        # Evaluation
        final_accuracy = evaluate_model(model, test_loader)
        print(f"Final Accuracy after fine-tuning on new data: {final_accuracy:.4f}")
        
        # Log metrics
        mlflow.log_metric("accuracy", final_accuracy)
        mlflow.log_metric("new_samples_trained", len(full_pdf))
        
        # Update Watermark
        mlflow.set_tag("last_timestamp", new_watermark)
        print(f"New watermark set to: {new_watermark}")

        # Free large in-memory objects before serializing model to MLflow
        # (avoids OOM when Spark residual heap + tensors + mlflow upload all run at once)
        del train_loader, test_loader, train_ds, test_ds, dataset, X, y, full_pdf
        gc.collect()
        print("Freed training tensors. Logging model to MLflow...")

        # 6. Logging Final Model to MLflow
        # (Pass empty pip_requirements to avoid a memory-heavy pip subprocess that causes OOM)
        mlflow.pytorch.log_model(model, "model", registered_model_name=MODEL_NAME, pip_requirements=[])

        
        # Promotion Logic
        latest_unassigned = client.get_latest_versions(MODEL_NAME, stages=["None"])
        if latest_unassigned:
            challenger_info = latest_unassigned[0]
            improvement = final_accuracy - champion_accuracy
            should_promote = improvement > PROMOTION_ACCURACY_THRESHOLD
            if force and not should_promote:
                print(f"WARNING: --force flag used. Challenger (Acc: {final_accuracy:.4f}) does NOT beat "
                      f"champion (Acc: {champion_accuracy:.4f}) by >{PROMOTION_ACCURACY_THRESHOLD:.0%}. "
                      f"Promoting anyway due to --force.")
            if force or should_promote:
                print(f"PROMOTING Version {challenger_info.version} (Acc: {final_accuracy:.4f})")
                if champion_version:
                    client.transition_model_version_stage(MODEL_NAME, champion_version.version, "Archived")
                client.transition_model_version_stage(MODEL_NAME, challenger_info.version, "Production")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--incremental", action="store_true")
    args = parser.parse_args()
    main(args.limit, args.force, args.incremental)
