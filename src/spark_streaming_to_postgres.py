from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
import os
from dotenv import load_dotenv

load_dotenv()  

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DATA_DIR = os.getenv("DATA_DIR")

# Initialize Spark with PostgreSQL JDBC
spark = SparkSession.builder \
    .appName("EcommerceStreaming") \
    .config("spark.jars", "/home/airlectric/Amalitech_projects/DEM05_Big_Data_Spark_Airflow/real-time-ecommerce-pipeline/lib/postgresql-42.7.1.jar") \
    .getOrCreate()


# Read streaming CSVs (schema inference for simplicity; will define schema explicitly for prod)
schema = "user_id LONG, action STRING, product_id LONG, product_name STRING, price DOUBLE, timestamp STRING"
streaming_df = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .option("header", "true") \
    .load(DATA_DIR)

# Transformations: Clean and convert types (lazy evaluation)
transformed_df = streaming_df \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .filter(col("action").isNotNull())  # Simple cleaning

# Write to PostgreSQL using foreachBatch (efficient for relational DBs)
def write_batch(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}") \
        .option("dbtable", "events") \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

query = transformed_df.writeStream \
    .foreachBatch(write_batch) \
    .outputMode("append") \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()  # Run until stopped