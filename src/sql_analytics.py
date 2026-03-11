import argparse
from pyspark.sql import SparkSession
import sys

def run_analytics(query_type, limit=20):
    """
    Run SQL analytics on HDFS data.
    """
    spark = SparkSession.builder \
        .appName("BDP2-SQL-Analytics") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Register HDFS data as a table
    data_path = "hdfs://namenode:9000/data/raw"
    try:
        df = spark.read.parquet(data_path)
        df.createOrReplaceTempView("mnist_data")
        print(f"✅ HDFS Data loaded into 'mnist_data' table.")
    except Exception as e:
        print(f"❌ Error loading data from HDFS: {e}")
        return

    # 2. Execute selected query
    if query_type == "summary":
        print("\n--- Digit Distribution (SQL GROUP BY) ---")
        query = "SELECT label, COUNT(*) as count FROM mnist_data GROUP BY label ORDER BY label"
    elif query_type == "sample":
        print(f"\n--- Random Data Sample (LIMIT {limit}) ---")
        query = f"SELECT * FROM mnist_data LIMIT {limit}"
    else:
        # Custom query support
        query = query_type

    print(f"Running SQL: {query}")
    try:
        result = spark.sql(query)
        result.show(limit)
    except Exception as e:
        print(f"❌ SQL Execution Error: {e}")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BDP2 SQL Analytics")
    parser.add_argument("--query", type=str, default="summary", 
                        help="Query type ('summary', 'sample') or raw SQL string.")
    parser.add_argument("--limit", type=int, default=20, 
                        help="Limit for results.")
    
    args = parser.parse_args()
    run_analytics(args.query, args.limit)
