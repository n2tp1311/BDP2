from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def main():
    spark = SparkSession.builder \
        .appName("DataIngestion") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Define schema for MNIST (64 pixel features + label + produce_timestamp for latency tracking)
    fields = [StructField(f"pixel_{i}", DoubleType()) for i in range(64)]
    fields.append(StructField("label", IntegerType()))
    fields.append(StructField("produce_timestamp", DoubleType()))  # Unix epoch (seconds)
    schema = StructType(fields)

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "input_data") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON, add ingest_timestamp - produce_timestamp difference = latency in ms
    from pyspark.sql.functions import unix_timestamp
    parsed_df = (
        df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            current_timestamp().alias("ingest_timestamp"),
        )
        .select("data.*", "ingest_timestamp")
        .withColumn(
            "latency_ms",
            (unix_timestamp(col("ingest_timestamp")) - col("produce_timestamp")) * 1000,
        )
    )

    # Write to HDFS in Parquet format
    query = parsed_df.writeStream \
        .format("parquet") \
        .option("path", "hdfs://namenode:9000/data/raw") \
        .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/data_ingestion") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
