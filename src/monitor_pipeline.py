import time
from kafka import KafkaConsumer, TopicPartition
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as spark_max


def get_kafka_counts(topic="input_data", bootstrap_servers="kafka:29092"):
    """Get the total number of messages produced to a Kafka topic (end offsets sum)."""
    try:
        consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return 0
        tp_list = [TopicPartition(topic, p) for p in partitions]
        end_offsets = consumer.end_offsets(tp_list)
        return sum(end_offsets.values())
    except Exception as e:
        print(f"  ⚠️  Kafka error: {e}")
        return -1


def get_hdfs_metrics(spark):
    """
    Query HDFS Parquet for:
    - Total record count
    - Average latency_ms across all records
    - Average latency_ms over the last 30 seconds (recent)
    """
    data_path = "hdfs://namenode:9000/data/raw"
    try:
        df = spark.read.parquet(data_path)
        total = df.count()

        # Average end-to-end latency over all records
        avg_latency_all = df.select(avg("latency_ms")).collect()[0][0]

        # Recent latency: records ingested in the last 30 seconds
        cutoff = time.time() - 30  # produce_timestamp cutoff
        recent_df = df.filter(col("produce_timestamp") >= cutoff)
        avg_latency_recent = recent_df.select(avg("latency_ms")).collect()[0][0]

        return total, avg_latency_all, avg_latency_recent
    except Exception as e:
        if "Path does not exist" in str(e):
            return 0, None, None
        print(f"  ⚠️  HDFS error: {e}")
        return -1, None, None


def main():
    spark = SparkSession.builder \
        .appName("BDP2-Monitor") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n" + "=" * 50)
    print("       📊  BDP2 PIPELINE MONITOR  📊")
    print("=" * 50)

    prev_hdfs_count = 0
    prev_time = time.time()

    while True:
        try:
            now = time.time()
            elapsed = now - prev_time

            kafka_total = get_kafka_counts()
            hdfs_total, avg_latency_all, avg_latency_recent = get_hdfs_metrics(spark)

            # Throughput: new HDFS records in this interval / elapsed seconds
            new_records = max(0, hdfs_total - prev_hdfs_count)
            throughput = new_records / elapsed if elapsed > 0 else 0.0

            print(f"\n[{time.strftime('%H:%M:%S')}]")
            print(f"  🔹 Kafka  (Produced):  {kafka_total:,} messages")
            print(f"  🔸 HDFS   (Persisted): {hdfs_total:,} records")

            if kafka_total > 0:
                percent = (hdfs_total / kafka_total) * 100
                print(f"  ✅ Sync   Progress:    {percent:.1f}%")

            # Throughput
            print(f"\n  ⚡ Throughput:          {throughput:.2f} records/sec  "
                  f"(+{new_records} in last {elapsed:.0f}s)")

            # End-to-end latency
            if avg_latency_all is not None:
                print(f"  🕐 Latency (all):      {avg_latency_all:,.0f} ms avg")
            else:
                print(f"  🕐 Latency:            N/A (no latency_ms column yet)")

            if avg_latency_recent is not None:
                print(f"  🕑 Latency (last 30s): {avg_latency_recent:,.0f} ms avg")

            print("-" * 50)

            prev_hdfs_count = hdfs_total if hdfs_total >= 0 else prev_hdfs_count
            prev_time = now
            time.sleep(10)

        except KeyboardInterrupt:
            print("\nMonitoring stopped.")
            break
        except Exception as e:
            print(f"Monitor loop error: {e}")
            time.sleep(5)

    spark.stop()


if __name__ == "__main__":
    main()
