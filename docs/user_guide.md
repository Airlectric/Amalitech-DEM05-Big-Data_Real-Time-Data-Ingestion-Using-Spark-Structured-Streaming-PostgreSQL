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
SPARK_MASTER=local[*]
```

> ⚠️ **Do not commit `.env` to GitHub**

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
SELECT COUNT(*) FROM events;

SELECT * FROM events LIMIT 10;
```

You should see rows increasing as Spark processes batches.

---

## 10. Logs & Monitoring

### View Container Logs

```bash
docker compose logs -f spark
docker compose logs -f postgres
```

### Spark UI

If port 4040 is exposed:

```text
http://localhost:4040
```

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

Happy Streaming 🚀
