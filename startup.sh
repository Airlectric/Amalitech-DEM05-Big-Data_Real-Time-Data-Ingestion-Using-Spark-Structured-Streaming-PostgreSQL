#!/bin/bash
set -e   # Stop if any command fails

echo "Waiting 10s for Postgres to be ready..."
sleep 10

echo "Running SQL setup..."
psql "postgresql://${DB_USER}:${DB_PASS}@${DB_HOST}:${DB_PORT}/${DB_NAME}" -f /opt/spark/app/sql/postgres_setup.sql

# Create directories for streaming data
mkdir -p /opt/spark/app/data/events

echo "Generating CSV data and Starting Spark streaming job..."
python3 /opt/spark/app/src/data_generator.py &

spark-submit \
  --master ${SPARK_MASTER} \
  --jars /opt/spark/app/lib/postgresql-42.7.1.jar \
  /opt/spark/app/src/spark_streaming_to_postgres.py


# wait for the background data generator process
wait