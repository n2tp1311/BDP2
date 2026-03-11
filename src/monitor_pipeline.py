import time
from kafka import KafkaConsumer, TopicPartition
from pyspark.sql import SparkSession
import sys
import os

def get_kafka_counts(topic="input_data", bootstrap_servers="kafka:29092"):
    """Get the current end offsets for Kafka partitions."""
    try:
        consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return 0
        
        tp_list = [TopicPartition(topic, p) for p in partitions]
        end_offsets = consumer.end_offsets(tp_list)
        total_messages = sum(end_offsets.values())
        return total_messages
    except Exception as e:
        print(f"⚠️ Kafka Monitoring Error: {e}")
        return -1

def get_hdfs_counts(spark):
    """Get the row count from HDFS Parquet data."""
    data_path = "hdfs://namenode:9000/data/raw"
    try:
        df = spark.read.parquet(data_path)
        return df.count()
    except Exception as e:
        # If path doesn't exist yet, it's 0
        if "Path does not exist" in str(e):
            return 0
        print(f"⚠️ HDFS Monitoring Error: {e}")
        return -1

def main():
    spark = SparkSession.builder \
        .appName("BDP2-Monitor") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    print("\n" + "="*40)
    print("      📊 BDP2 PIPELINE MONITOR 📊")
    print("="*40)
    
    while True:
        try:
            kafka_total = get_kafka_counts()
            hdfs_total = get_hdfs_counts(spark)
            
            print(f"[{time.strftime('%H:%M:%S')}]")
            print(f"  🔹 Kafka (Generated): {kafka_total:,} samples")
            print(f"  🔸 HDFS (Persisted):  {hdfs_total:,} samples")
            
            if kafka_total > 0:
                percent = (hdfs_total / kafka_total) * 100 if kafka_total > 0 else 0
                print(f"  ✅ Sync Progress:     {percent:.1f}%")
            
            print("-" * 30)
            time.sleep(10) # Refresh every 10 seconds
        except KeyboardInterrupt:
            print("\nMonitoring stopped.")
            break
        except Exception as e:
            print(f"Monitor Loop Error: {e}")
            time.sleep(5)

    spark.stop()

if __name__ == "__main__":
    main()
