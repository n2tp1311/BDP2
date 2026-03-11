import os
import subprocess
import time

def run_cmd(cmd, ignore_error=False):
    print(f"Executing: {cmd}")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        if not ignore_error:
            msg = e.stderr.strip() if e.stderr else "Unknown error (check container logs)"
            print(f"⚠️ Warning: {msg}")
        return None

def reset_pipeline():
    print("🧹 Starting Global Pipeline Reset (Host-side)...")

    # 0. Kill stale processes
    print("\n--- 0. Stopping running scripts ---")
    run_cmd("docker exec -u 0 spark-master pkill -9 -f python3 || true")
    run_cmd("docker exec -u 0 spark-master pkill -9 -f spark-submit || true")
    run_cmd("docker exec -u 0 spark-worker pkill -9 -f python3 || true")

    # 1. Reset HDFS Data & Checkpoints
    print("\n--- 1. Resetting HDFS ---")
    run_cmd("docker exec namenode hdfs dfs -rm -r -f /data/raw")
    run_cmd("docker exec namenode hdfs dfs -rm -r -f /checkpoints/data_ingestion")
    run_cmd("docker exec namenode hdfs dfs -mkdir -p /data/raw")

    # 2. Reset Kafka Topic
    print("\n--- 2. Resetting Kafka ---")
    run_cmd("docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic input_data", ignore_error=True)
    
    # Wait for deletion (up to 10 seconds)
    print("Waiting for topic deletion...")
    for _ in range(10):
        topics = run_cmd("docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list", ignore_error=True)
        if topics is None or "input_data" not in topics:
            break
        time.sleep(1)
    
    run_cmd("docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic input_data --partitions 1 --replication-factor 1")

    # 3. Reset MLflow (Wipe SQL DB and Artifacts)
    print("\n--- 3. Resetting MLflow ---")
    # This wipes all runs and experiments in the Postgres DB
    run_cmd('docker exec mlflow-db psql -U mlflow -d mlflow -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"')
    # This wipes all physical model files
    run_cmd("docker exec mlflow-server rm -rf /mlflow-artifacts/*")

    print("\n✨ Reset Complete! The BDP2 platform is now in a clean state.")

if __name__ == "__main__":
    # Check if we are inside a container
    if os.path.exists("/.dockerenv"):
        print("❌ Error: Please run this script on your LOCAL MACHINE (Host), not inside a container.")
    else:
        reset_pipeline()
