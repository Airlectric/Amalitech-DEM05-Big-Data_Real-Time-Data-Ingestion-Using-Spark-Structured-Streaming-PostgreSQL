# Test Cases for Real-Time E-Commerce Streaming Pipeline

This document lists manual test cases for the e-commerce streaming project.
It covers the data generator, Spark Structured Streaming transformations, and PostgreSQL sink.

| Test Case ID | Component | Description | Steps to Execute | Expected Outcome | Actual Outcome |
|--------------|-----------|-------------|-----------------|-----------------|----------------|
| TC001 | Data Generator | Generate CSV files | Run `python src/data_generator.py` | CSV files appear in `data/events/` folder with correct columns | Verified |
| TC002 | Data Generator | CSV content correctness | Open a CSV, check column names & types | Columns: user_id, action, product_id, product_name, price, timestamp, session_id | Verified |
| TC003 | Spark Streaming | Ingestion of new files | Start Spark job `spark_streaming_to_postgres.py` while generator runs | Streaming job reads new CSV files continuously | Verified |
| TC004 | Spark Streaming | Timestamp parsing | Check `event_time` column in Spark/DB | ISO and HH:mm:ss timestamps parsed correctly, fallback to ingestion time if invalid | Verified |
| TC005 | Spark Streaming | Action standardization | Check `action` column | Values normalized to uppercase; invalid actions become "UNKNOWN" | Verified |
| TC006 | Spark Streaming | Price handling | Check `price` column | Negative prices replaced with 0 | Verified |
| TC007 | Spark Streaming | User/Product ID cleaning | Check `user_id` and `product_id` | Only positive IDs preserved, invalid IDs are null | Verified |
| TC008 | Spark Streaming | Product name cleaning | Check `product_name` column | Null or empty names replaced with "Unknown Product" | Verified |
| TC009 | Spark Streaming | Session ID handling | Check `session_id` column | Null or missing values replaced with "unknown" | Verified |
| TC010 | PostgreSQL | Database write | Query `events` table during streaming | New records inserted with all columns properly mapped | Verified |
| TC011 | Streaming Fault Tolerance | Stop generator for a batch and restart | Streaming continues without error | Spark picks up new files correctly | Verified |
| TC012 | Check Logging | Spark log | Check Spark log file (`spark.log`) | All job info, errors, and batch prints captured in the log file | Verified |
| TC013 | Check Logging | PostgreSQL log | Insert new batch, check `pg_log` | PostgreSQL logs queries and inserts | Verified |
