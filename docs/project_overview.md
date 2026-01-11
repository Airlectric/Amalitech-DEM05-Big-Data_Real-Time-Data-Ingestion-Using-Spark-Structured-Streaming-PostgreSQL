# Project Overview

This system simulates a real-time e-commerce user activity tracker.

**Components and Flow:**
1. **Data Generation**: `data_generator.py` creates CSV files with fake events (user_id, action, etc.) in `data/events/`. Runs in a loop to mimic streaming.
2. **Streaming Processing**: `spark_streaming_to_postgres.py` uses Spark Structured Streaming to watch the folder, read new CSVs, transform (e.g., timestamp conversion), and batch-write to PostgreSQL.
3. **Storage**: PostgreSQL stores events in `events` table for querying.
4. **Architecture**: Producer (generator) → File System (CSVs) → Spark Streaming (processor) → PostgreSQL (sink).

This pipeline handles continuous data efficiently with low latency (~5s batches).