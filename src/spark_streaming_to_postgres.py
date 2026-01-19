from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, expr, lit, current_timestamp,
    coalesce, trim, upper, regexp_replace
)
from pyspark.sql.types import TimestampType
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DATA_DIR = os.getenv("DATA_DIR", "/opt/spark/app/data/events")

# Initialized Spark with PostgreSQL JDBC
spark = SparkSession.builder \
    .appName("EcommerceRealTimeIngestion") \
    .config("spark.jars", "/opt/spark/app/lib/postgresql-42.7.1.jar") \
    .config("spark.sql.streaming.checkpointLocation", "/opt/spark/app/checkpoint") \
    .getOrCreate()

# Defined expected schema
schema = """
    user_id LONG,
    action STRING,
    product_id LONG,
    product_name STRING,
    price DOUBLE,
    timestamp STRING,
    session_id STRING
"""

print("Starting streaming ingestion from:", DATA_DIR)

streaming_df = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .load(DATA_DIR)

# === Real-world quality transformations ===
cleaned_df = ( 
    streaming_df 

    # 1. Timestamp parsing - trying multiple formats
    .withColumn(
        "event_time",
        coalesce(
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
            to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(lit(None), "yyyy-MM-dd HH:mm:ss")  # null if both fail
        )
    )
    # Fallback to ingestion time if timestamp is invalid/missing
    .withColumn(
        "event_time",
        coalesce(col("event_time"), current_timestamp())
    )
    
    # 2. Clean action - standardize & handle null/empty
    .withColumn(
        "action",
        upper(trim(coalesce(col("action"), lit("UNKNOWN"))))
    )

    .withColumn(
        "action",
        when(col("action").isin("VIEW", "PURCHASE", "ADD_TO_CART", "REMOVE_FROM_CART"), col("action"))
         .otherwise("UNKNOWN")
    )

    # 3. Fix prices - no negatives, replacing with 0
    .withColumn("price", when(col("price") >= 0, col("price")).otherwise(0.0))
    
    # 4. Clean IDs - replace invalid with null
    .withColumn("user_id", when(col("user_id").isNotNull() & (col("user_id") > 0), col("user_id")))
    .withColumn("product_id", when(col("product_id") > 0, col("product_id")))
    
    # 5. Clean product name
    .withColumn("product_name", trim(coalesce(col("product_name"), lit("Unknown Product"))))
    
    # 6. Handle session_id (may be missing)
    .withColumn("session_id", coalesce(col("session_id"), lit("unknown")))
)

# Final columns to be stored
final_df = cleaned_df.select(
    "user_id",
    "action",
    "product_id",
    "product_name",
    "price",
    "event_time",
    "session_id"
)

# Writing to PostgreSQL using foreachBatch
def write_batch(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}") \
            .option("dbtable", "events") \
            .option("user", DB_USER) \
            .option("password", DB_PASS) \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", "2000") \
            .option("checkpointLocation", "/opt/spark/app/checkpoint/events") \
            .mode("append") \
            .save()
        
        print(f"Batch {batch_id} written - {batch_df.count()} rows")

query = final_df.writeStream \
    .foreachBatch(write_batch) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Streaming query started. Waiting for termination...")
query.awaitTermination()