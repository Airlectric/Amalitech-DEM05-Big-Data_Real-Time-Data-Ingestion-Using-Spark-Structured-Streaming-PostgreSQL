# User Guide – Real-Time Ecommerce Data Pipeline

This guide explains how to set up, run, and verify the **Real-Time Ecommerce Data Ingestion Pipeline** using **Docker, Spark Structured Streaming, and PostgreSQL**.

---

## 1. Prerequisites

Ensure the following are installed on your system:

* **Docker & Docker Compose** (v2 recommended)
* **Git**
* (Optional for local runs) **Java 11+**, **Python 3.9+**, **Apache Spark 3.5+**

Verify:

```bash
docker --version
docker compose version
```

---

## 2. Project Structure Overview

```text
├── .env
├── .env.example
├── .gitignore
├── README.md
├── data
│   └── events
│       ├── events_batch_0.csv
│       ├── events_batch_1.csv
│       ├── events_batch_10.csv
│       ├── events_batch_11.csv
│       ├── events_batch_12.csv
│       ├── events_batch_13.csv
│       ├── events_batch_14.csv
│       ├── events_batch_15.csv
│       ├── events_batch_16.csv
│       ├── events_batch_17.csv
│       ├── events_batch_18.csv
│       ├── events_batch_19.csv
│       ├── events_batch_2.csv
│       ├── events_batch_3.csv
│       ├── events_batch_4.csv
│       ├── events_batch_5.csv
│       ├── events_batch_6.csv
│       ├── events_batch_7.csv
│       ├── events_batch_8.csv
│       └── events_batch_9.csv
├── docker
│   └── Dockerfile.spark
├── docker-compose.yml
├── docs
│   ├── project_overview.md
│   ├── test_cases.md
│   └── user_guide.md
├── lib
│   └── postgresql-42.7.1.jar
├── logs
│   ├── postgres
│   │   └── postgres.log
│   └── spark
│       └── spark_pipeline.log
├── requirements.txt
├── sql
│   └── postgres_setup.sql
├── src
│   ├── data_generator.py
│   └── spark_streaming_to_postgres.py
├── startup.sh
```

---

## 3. Environment Configuration

Create a `.env` file in the project root:

```env
DB_NAME=ecommerce_database
DB_USER=data_user
DB_PASS=strongpassword
DB_HOST=postgres
DB_PORT=5432
SPARK_MASTER=spark://spark-master:7077
```

> ⚠️ **Do not commit `.env` to GitHub**
>
> **Note**: The pipeline runs on a Spark cluster (spark-master) instead of local mode for better monitoring and scalability.

---

## 4. PostgreSQL Setup (Dockerized)

PostgreSQL is automatically initialized via Docker Compose.

The SQL file `sql/postgres_setup.sql`:

* Connects to `ecommerce_database`
* Creates the `events` table
* Creates indexes on `timestamp`, `user_id`, and `action`

No manual DB setup is required when using Docker.

---

## 5. Spark Image & JDBC Driver

The Spark Docker image:

* Is based on `apache/spark:3.5.1`
* Installs `postgresql-client`
* Includes the PostgreSQL JDBC driver in `/opt/spark/app/lib/`

Spark **does not auto-load JDBC drivers**, so it must be explicitly passed during submission.

---

## 6. Running the Pipeline (Recommended – Docker)

### Step 1: Build and Start Services

```bash
docker compose down -v
docker compose up --build
```

This will:

* Start PostgreSQL
* Run SQL setup scripts
* Start the CSV data generator
* Launch Spark Structured Streaming

---

## 7. What Happens Internally

### Data Generator (`data_generator.py`)

* Generates **20 CSV files**
* Each file contains **100 events**
* Files arrive every **5 seconds**
* Output path:

```text
/opt/spark/app/data/events/
```

### Spark Streaming Job

* Monitors the CSV directory
* Processes new files as micro-batches
* Writes data to PostgreSQL using JDBC
* Uses **Structured Streaming (file source)**

---

## 8. Spark Submission Command (Important)

Inside `startup.sh`, Spark is launched as:

```bash
spark-submit \
  --master ${SPARK_MASTER} \
  --jars /opt/spark/app/lib/postgresql-42.7.1.jar \
  /opt/spark/app/src/spark_streaming_to_postgres.py
```

### Why this matters

* `--master` → execution environment
* `--jars` → prevents `ClassNotFoundException: org.postgresql.Driver`

---

## 9. Verifying Data Ingestion

### Connect to PostgreSQL

```bash
docker exec -it postgres psql -U data_user -d ecommerce_database
```

### Run Validation Queries

```sql
-- Verify tables exist
\dt

-- Check schema
SCHEMA |      Name       | Type  |   Owner
--------+-----------------+-------+-----------
 public | corrupt_records | table | data_user
 public | events          | table | data_user

-- Count total events
SELECT COUNT(*) FROM events;

-- View recent events
SELECT * FROM events LIMIT 10;

-- Check for corrupt records
SELECT COUNT(*) FROM corrupt_records;
SELECT * FROM corrupt_records LIMIT 5;
```

You should see rows increasing as Spark processes batches.

### Database Schema Details

**events table**: Stores all validated e-commerce event records with columns for user_id, action, product_id, product_name, price, event_time, session_id, and ingestion_time.

**corrupt_records table**: Stores records that failed CSV parsing validation, preserving the raw corrupt data for debugging and data quality analysis.

---

## 10. Logs & Monitoring

### View Container Logs

```bash
docker compose logs -f spark
docker compose logs -f postgres
docker compose logs -f spark-master
```

### Spark Web UIs

After starting the pipeline, access the following monitoring interfaces:

**Spark Master UI** - http://localhost:8080
- Cluster overview and resource allocation
- Active and completed applications
- Worker nodes status
- Running drivers and executors

**Spark Application UI** - http://localhost:4040
- Real-time job execution details
- Streaming metrics (input rate, processing time)
- SQL query plans and stage information
- Executor and storage metrics

**Spark History Server** - http://localhost:18080
- Historical application data
- Completed job analysis
- Event logs and performance timeline

---

## 11. Common Issues & Fixes

### ❌ Database is empty

* Ensure Spark job did not crash
* Check for JDBC driver errors
* Confirm CSV files exist in `/data/events`

### ❌ `ClassNotFoundException: org.postgresql.Driver`

✅ Fix:

```bash
--jars /opt/spark/app/lib/postgresql-42.7.1.jar
```

### ❌ Streaming stays idle

* Generator may have finished
* Spark waits indefinitely for new files (expected behavior)

---

## 12. Stopping the Pipeline

```bash
CTRL+C
docker compose down
```

To remove volumes:

```bash
docker compose down -v
```

---

## 13. Production Notes

* Use **persistent checkpoints** (not `/tmp`)
* Externalize configs (Vault, Secrets Manager)
* Add retries & backpressure
* Use Parquet + Kafka for scale

---

## 14. Summary

✅ Real-time CSV ingestion

✅ Spark Structured Streaming

✅ PostgreSQL sink via JDBC

✅ Dockerized, reproducible setup

This project demonstrates **real-world streaming fundamentals** and common Spark pitfalls (JDBC, checkpoints, parallel jobs).

---

## 15. Testing

The project includes comprehensive automated tests to validate functionality, data quality, and performance.

### Test Categories

**Unit Tests (26 tests)** - Validate individual components without running services:
- Data generator functions (10 tests)
- CSV batch generation (5 tests)
- PySpark transformations (8 tests)
- Schema and configuration (3 tests)

**Integration Tests (6 tests)** - End-to-end validation with running services:
- Database connection
- Full pipeline flow (CSV → Spark → PostgreSQL)
- Data quality with actual transformations
- Performance baseline
- Concurrent file processing
- Constraint validation

### Running Tests

**Quick Start:**
```bash
# Run all tests (unit + integration)
bash run_tests.sh all

# Run unit tests only (fast, no services needed)
bash run_tests.sh unit

# Run integration tests only (requires postgres + spark)
bash run_tests.sh integration

# View test logs
bash run_tests.sh logs
```

**Using Docker Compose:**
```bash
# All tests with profile
docker-compose --profile test up --abort-on-container-exit

# Unit tests only
docker-compose run --rm test pytest tests/test_pipeline.py -v

# Integration tests (start services first)
docker-compose up -d postgres spark
sleep 30
docker-compose run --rm test pytest tests/test_integration.py -v
docker-compose down
```

### Test Structure
```
tests/
├── conftest.py              # Shared fixtures (spark_session, db_connection)
├── test_pipeline.py         # 26 unit tests
└── test_integration.py      # 6 integration tests
```

### Expected Results
- **Execution Time**: ~78 seconds total (9s unit + 69s integration)
- **Success Rate**: 100% (32/32 tests passing)
- **Coverage**: 87.5% of test cases validated

For detailed test cases and manual testing procedures, see [test_cases.md](test_cases.md).

---

## 16. Performance Monitoring

The pipeline includes built-in performance monitoring through Spark UI and REST APIs.

### Monitoring Endpoints

After starting the pipeline, access the following URLs:

**1. Spark Master UI** - http://localhost:8080
- Cluster status and resource overview
- Active workers and their resources (cores, memory)
- Running and completed applications
- Submitted drivers and their status
- Cluster-wide metrics and health

**2. Spark Application UI** - http://localhost:4040
- Real-time streaming metrics (input rate, processing time, batch duration)
- Job execution timeline and DAG visualization
- SQL query execution plans
- Executor metrics (memory, CPU, tasks)

**3. Spark History Server** - http://localhost:18080
- Historical view of completed applications
- Performance analysis after pipeline stops
- Useful for comparing multiple runs

**4. Spark REST API** - http://localhost:4040/api/v1/applications/
- Programmatic access to all metrics in JSON format
- Streaming statistics (avgInputRate, avgProcessingTime, avgSchedulingDelay)
- Batch-level details

### Quick Access URLs

Once the pipeline is running:
```bash
# Open monitoring interfaces
echo "Spark Master UI: http://localhost:8080"
echo "Spark Application UI: http://localhost:4040"
echo "Spark History Server: http://localhost:18080"
echo "PostgreSQL: localhost:5432"

# Or access directly in browser
open http://localhost:8080  # Spark Master
open http://localhost:4040  # Application UI
open http://localhost:18080 # History Server
```

### Key Performance Metrics

**From Spark Master UI (Port 8080):**
- **Workers**: Number of active worker nodes
- **Cores**: Total available and used CPU cores
- **Memory**: Cluster memory allocation
- **Applications**: Running and completed application count

**From Spark Application UI (Port 4040):**
- **Input Rate**: Records received per second (~50-60 records/sec expected)
- **Processing Rate**: Records processed per second (500-600 records/sec expected)
- **Batch Duration**: Time to complete one micro-batch (<2s expected)
- **Scheduling Delay**: Time waiting for previous batch (should be ~0)

---

Happy Streaming 🚀
