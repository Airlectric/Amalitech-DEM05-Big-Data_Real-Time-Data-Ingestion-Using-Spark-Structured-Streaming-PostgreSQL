# User Guide

1. **Setup PostgreSQL**:
   - Install PostgreSQL (e.g., via brew/apt).
   - Create user/db: `createuser your_user; createdb ecommerce_db -O your_user`.
   - Run `psql -d ecommerce_db -f ../sql/postgres_setup.sql`.

2. **Update Configs**:
   - Fill `config/postgres_connection_details.txt`.
   - In `spark_streaming_to_postgres.py`, update DB creds and JDBC path.

3. **Run Data Generator**:
   - `cd src/ && python data_generator.py` (generates 10 files, 5s apart).

4. **Run Spark Job**:
   - In another terminal: `spark-submit --driver-class-path /path/to/postgresql-42.7.1.jar spark_streaming_to_postgres.py`.

5. **Verify**:
   - Query DB: `psql -d ecommerce_db -c "SELECT * FROM events LIMIT 10;"`.
