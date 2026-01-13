
# Real-Time E-commerce Events Pipeline  
Apache Spark Structured Streaming to PostgreSQL

**Module Lab 2** – Real-time data ingestion and processing of simulated e-commerce user activity

### Overview

This project demonstrates a complete near-real-time data pipeline that:
- Generates synthetic e-commerce events with realistic data quality issues
- Streams and processes them using **Spark Structured Streaming**
- Cleans/transforms the data (timestamps, nulls, casing, invalid values, schema evolution)
- Persists results into **PostgreSQL**

### Project Structure

```
real-time-ecommerce-pipeline/
├── src/
│   ├── data_generator.py                    → Event generator (with intentional data issues)
│   └── spark_streaming_to_postgres.py       → Streaming job + transformations + PostgreSQL sink
├── sql/
│   └── postgres_setup.sql                   → Database, table, constraints & indexes
├── config/
│   └── postgres_connection_details.txt      → DB connection template (do NOT commit!)
├── docs/                                    → All deliverable documentation
│   ├── project_overview.md
│   ├── user_guide.md
│   ├── test_cases.md
│   ├── performance_metrics.md
│   └── system_architecture.png
├── data/
│   └── events/                              → Generated CSV files (ephemeral)
├── requirements.txt
└── README.md                                ← You are here
```

### Documentation & Deliverables

All detailed documentation lives in the `docs/` folder:

| Document                        | Description                                                  | Link                                      |
|---------------------------------|--------------------------------------------------------------|-------------------------------------------|
| Project Overview                | System architecture, components & data flow explanation      | [docs/project_overview.md](docs/project_overview.md) |
| User Guide                      | Complete setup, configuration & running instructions         | [docs/user_guide.md](docs/user_guide.md)         |
| Test Cases                      | Manual test plan + expected vs actual results                | [docs/test_cases.md](docs/test_cases.md)         |
| Performance Metrics Report      | Throughput, latency, resource usage & stability evaluation   | [docs/performance_metrics.md](docs/performance_metrics.md) |


### Quick Start (minimal version)

1. Create & configure PostgreSQL database  
   ```bash
   psql -f sql/postgres_setup.sql
   ```

2. Fill connection details in `config/postgres_connection_details.txt` (or use env variables)

3. Start data generator (one terminal)  
   ```bash
   python src/data_generator.py
   ```

4. Launch Spark streaming job (another terminal)  
   ```bash
   spark-submit \
     --driver-class-path /path/to/postgresql-jdbc.jar \
     src/spark_streaming_to_postgres.py
   ```

For full instructions, environment setup, Docker notes, troubleshooting and verification steps →  
please read > **[docs/user_guide.md](docs/user_guide.md)**

### Learning Objectives Covered

- Streaming data simulation
- Spark Structured Streaming with file source
- Real-world data quality handling & cleaning
- Efficient JDBC sink using foreachBatch
- Performance measurement & evaluation
- Structured project organization & documentation

Happy streaming!  
