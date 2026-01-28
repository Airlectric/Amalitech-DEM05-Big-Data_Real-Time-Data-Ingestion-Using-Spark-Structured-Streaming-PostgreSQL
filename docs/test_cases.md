# Test Cases for Real-Time E-Commerce Streaming Pipeline

This document provides a comprehensive test suite for the e-commerce streaming pipeline.
It covers data generation, Spark Structured Streaming transformations, PostgreSQL persistence, and performance metrics.

## Test Suite Overview

The test suite is organized into five main categories:
1. **CSV File Generation Tests** - Validate data generator output
2. **Spark File Detection and Processing Tests** - Verify streaming ingestion
3. **Data Transformation Tests** - Ensure correct data cleaning and transformation
4. **PostgreSQL Integration Tests** - Validate database writes and data integrity
5. **Performance and Monitoring Tests** - Measure throughput and latency

---

## Test Suite 1: CSV File Generation Tests

### TC-GEN-001: CSV File Creation
**Objective**: Verify that CSV files are generated in the correct directory

**Test Steps**:
1. Clean the `data/events/` directory
2. Run `python src/data_generator.py`
3. Check if CSV files are created

**Expected Result**: 
- 20 CSV files created in `data/events/` directory
- Files named as `events_batch_000.csv` to `events_batch_019.csv`

**Pass/Fail**: _____

---

### TC-GEN-002: CSV Schema Validation
**Objective**: Verify CSV files have correct column structure

**Test Steps**:
1. Generate CSV files
2. Read first CSV file and inspect headers
3. Validate column names and order

**Expected Result**:
- All files contain columns: `user_id`, `action`, `product_id`, `product_name`, `price`, `timestamp`, `session_id`
- Header row present in all files

**Pass/Fail**: _____

---

### TC-GEN-003: CSV Data Type Validation
**Objective**: Verify data types in generated CSV files

**Test Steps**:
1. Generate CSV files
2. Read sample records from multiple files
3. Validate data types for each column

**Expected Result**:
- `user_id`: Integer or null
- `action`: String (view, purchase, add_to_cart, remove_from_cart, or variations)
- `product_id`: Integer or -1
- `product_name`: String
- `price`: Float (may include negative values)
- `timestamp`: String (various formats or empty)
- `session_id`: String or null

**Pass/Fail**: _____

---

### TC-GEN-004: Data Quality Issues Present
**Objective**: Verify intentional data quality issues are generated

**Test Steps**:
1. Generate 20 batches of CSV files
2. Scan files for data quality issues
3. Count occurrences of each issue type

**Expected Result**:
- Some records have null `user_id`
- Some records have invalid `product_id` (-1)
- Some records have negative prices
- Some records have empty or invalid timestamps
- Some records have missing `session_id`
- Some records have mixed case actions

**Pass/Fail**: _____

---

### TC-GEN-005: Batch Size Validation
**Objective**: Verify each batch contains expected number of records

**Test Steps**:
1. Generate CSV files
2. Count rows in each file (excluding header)
3. Verify row count

**Expected Result**:
- Each CSV file contains 100 data rows (plus 1 header row)

**Pass/Fail**: _____

---

## Test Suite 2: Spark File Detection and Processing Tests

### TC-SPARK-001: Directory Monitoring
**Objective**: Verify Spark monitors the events directory

**Test Steps**:
1. Start Spark streaming job
2. Create a new CSV file in `data/events/`
3. Check Spark logs for file detection

**Expected Result**:
- Spark detects new file within trigger interval (10 seconds)
- Log shows file being added to processing queue

**Pass/Fail**: _____

---

### TC-SPARK-002: Continuous File Ingestion
**Objective**: Verify Spark processes multiple files continuously

**Test Steps**:
1. Start Spark streaming job
2. Run data generator to create files at intervals
3. Monitor Spark logs for batch processing

**Expected Result**:
- Spark processes each new batch within 10 seconds
- Multiple micro-batches completed successfully
- No files skipped

**Pass/Fail**: _____

---

### TC-SPARK-003: Schema Enforcement
**Objective**: Verify Spark enforces defined schema

**Test Steps**:
1. Create CSV with extra columns
2. Place in events directory
3. Check Spark processing behavior

**Expected Result**:
- Extra columns ignored
- Defined schema columns processed correctly
- No errors thrown

**Pass/Fail**: _____

---

### TC-SPARK-004: Checkpoint Recovery
**Objective**: Verify checkpoint mechanism for fault tolerance

**Test Steps**:
1. Start Spark job and process 5 batches
2. Stop Spark job
3. Restart Spark job
4. Add new files

**Expected Result**:
- Spark resumes from last checkpoint
- No duplicate processing of old files
- New files processed correctly

**Pass/Fail**: _____

---

## Test Suite 3: Data Transformation Tests

### TC-TRANS-001: Timestamp Parsing - ISO Format
**Objective**: Verify ISO timestamp format is parsed correctly

**Test Steps**:
1. Create record with ISO timestamp: `2026-01-28T10:30:45.123456`
2. Process through Spark
3. Query `event_time` from database

**Expected Result**:
- Timestamp parsed correctly
- `event_time` matches original timestamp
- No null values

**Pass/Fail**: _____

---

### TC-TRANS-002: Timestamp Parsing - Standard Format
**Objective**: Verify standard timestamp format is parsed correctly

**Test Steps**:
1. Create record with timestamp: `2026-01-28 10:30:45`
2. Process through Spark
3. Query `event_time` from database

**Expected Result**:
- Timestamp parsed correctly
- `event_time` matches original timestamp

**Pass/Fail**: _____

---

### TC-TRANS-003: Timestamp Fallback - Invalid
**Objective**: Verify fallback to current timestamp for invalid dates

**Test Steps**:
1. Create records with invalid timestamps: `INVALID_DATE`, empty string
2. Process through Spark
3. Query `event_time` from database

**Expected Result**:
- `event_time` is set to ingestion timestamp (current_timestamp)
- No null values in `event_time` column

**Pass/Fail**: _____

---

### TC-TRANS-004: Action Standardization - Uppercase
**Objective**: Verify actions are converted to uppercase

**Test Steps**:
1. Create records with mixed case actions: `view`, `VIEW`, `Purchase`
2. Process through Spark
3. Query `action` from database

**Expected Result**:
- All valid actions converted to uppercase: `VIEW`, `PURCHASE`
- Case normalization applied

**Pass/Fail**: _____

---

### TC-TRANS-005: Action Standardization - Invalid Values
**Objective**: Verify invalid actions are replaced with UNKNOWN

**Test Steps**:
1. Create records with invalid actions: empty string, null, `invalid_action`
2. Process through Spark
3. Query `action` from database

**Expected Result**:
- All invalid actions replaced with `UNKNOWN`
- No null or empty values in action column

**Pass/Fail**: _____

---

### TC-TRANS-006: Price Correction - Negative Values
**Objective**: Verify negative prices are replaced with 0

**Test Steps**:
1. Create records with negative prices: -10.50, -100.00
2. Process through Spark
3. Query `price` from database

**Expected Result**:
- All negative prices replaced with 0.0
- Positive prices unchanged

**Pass/Fail**: _____

---

### TC-TRANS-007: User ID Validation
**Objective**: Verify user ID handling for invalid values

**Test Steps**:
1. Create records with null, 0, and negative user IDs
2. Process through Spark
3. Query `user_id` from database

**Expected Result**:
- Null user IDs remain null
- Zero or negative user IDs converted to null
- Positive user IDs preserved

**Pass/Fail**: _____

---

### TC-TRANS-008: Product ID Validation
**Objective**: Verify product ID handling for invalid values

**Test Steps**:
1. Create records with -1, 0, and negative product IDs
2. Process through Spark
3. Query `product_id` from database

**Expected Result**:
- Invalid product IDs (less than or equal to 0) converted to null
- Positive product IDs preserved

**Pass/Fail**: _____

---

### TC-TRANS-009: Product Name Handling
**Objective**: Verify product name default for empty values

**Test Steps**:
1. Create records with empty and null product names
2. Process through Spark
3. Query `product_name` from database

**Expected Result**:
- Empty or null product names replaced with `Unknown Product`
- Valid product names preserved and trimmed

**Pass/Fail**: _____

---

### TC-TRANS-010: Session ID Handling
**Objective**: Verify session ID default for missing values

**Test Steps**:
1. Create records with null and missing session IDs
2. Process through Spark
3. Query `session_id` from database

**Expected Result**:
- Null or missing session IDs replaced with `unknown`
- Valid session IDs preserved

**Pass/Fail**: _____

---

## Test Suite 4: PostgreSQL Integration Tests

### TC-DB-001: Database Connection
**Objective**: Verify Spark can connect to PostgreSQL

**Test Steps**:
1. Start PostgreSQL container
2. Start Spark job
3. Check logs for connection success

**Expected Result**:
- Spark successfully connects to PostgreSQL
- No JDBC connection errors
- Database user authenticated

**Pass/Fail**: _____

---

### TC-DB-002: Table Write Operations
**Objective**: Verify records are written to events table

**Test Steps**:
1. Start pipeline with fresh database
2. Generate 1 batch (100 records)
3. Query database: `SELECT COUNT(*) FROM events;`

**Expected Result**:
- 100 records inserted into events table
- All columns populated correctly

**Pass/Fail**: _____

---

### TC-DB-003: Data Integrity
**Objective**: Verify written data matches transformed data

**Test Steps**:
1. Generate CSV with known values
2. Process through pipeline
3. Query database and compare values

**Expected Result**:
- All transformations reflected in database
- No data corruption
- Column mappings correct

**Pass/Fail**: _____

---

### TC-DB-004: Batch Insert Efficiency
**Objective**: Verify batch inserts are working

**Test Steps**:
1. Process large batch (500+ records)
2. Monitor database logs
3. Check for batch insert statements

**Expected Result**:
- Records inserted in batches (batch size: 2000)
- Efficient bulk inserts observed
- No individual INSERT statements for each record

**Pass/Fail**: _____

---

### TC-DB-005: Duplicate Prevention
**Objective**: Verify no duplicate records on restart

**Test Steps**:
1. Process 5 batches
2. Stop Spark job
3. Restart Spark job
4. Count records in database

**Expected Result**:
- No duplicate records inserted
- Record count matches expected (500 records for 5 batches)

**Pass/Fail**: _____

---

### TC-DB-006: Constraint Validation
**Objective**: Verify database constraints are enforced

**Test Steps**:
1. Process records through pipeline
2. Query records and check action values
3. Verify constraint compliance

**Expected Result**:
- All action values comply with CHECK constraint
- Only valid actions stored: VIEW, PURCHASE, ADD_TO_CART, REMOVE_FROM_CART, UNKNOWN

**Pass/Fail**: _____

---

### TC-DB-007: Index Usage
**Objective**: Verify indexes exist and are functional

**Test Steps**:
1. Insert records into database
2. Run query: `SELECT * FROM events WHERE action = 'VIEW' ORDER BY event_time DESC LIMIT 100;`
3. Check query execution plan

**Expected Result**:
- Query completes in under 100ms for 10,000 records
- Indexes on action and event_time utilized

**Pass/Fail**: _____

---

## Test Suite 5: Performance and Monitoring Tests

### TC-PERF-001: Data Generation Throughput
**Objective**: Measure data generation speed

**Test Steps**:
1. Run data generator for 20 batches
2. Record start and end time
3. Calculate throughput

**Expected Result**:
- Generation completes within 110 seconds (20 batches x 5 sec + processing)
- Throughput: approximately 15-20 events per second

**Pass/Fail**: _____

---

### TC-PERF-002: Spark Processing Rate
**Objective**: Measure Spark processing throughput

**Test Steps**:
1. Generate 2000 records
2. Start Spark job
3. Monitor Spark UI for processing rate

**Expected Result**:
- Processing rate: 500-600 records per second
- Input rate: 50-60 records per second

**Pass/Fail**: _____

---

### TC-PERF-003: Batch Processing Latency
**Objective**: Measure time to process each micro-batch

**Test Steps**:
1. Run pipeline
2. Check Spark UI batch duration
3. Record latency for 10 consecutive batches

**Expected Result**:
- Steady-state batch duration: 0.8-1.5 seconds
- Initial batch may take up to 5 seconds
- Consistent latency across batches

**Pass/Fail**: _____

---

### TC-PERF-004: End-to-End Latency
**Objective**: Measure time from file creation to database insert

**Test Steps**:
1. Note timestamp of CSV file creation
2. Query database for records from that file
3. Check ingestion_time of records

**Expected Result**:
- End-to-end latency: 10-15 seconds
- Records appear in database within 2 trigger intervals

**Pass/Fail**: _____

---

### TC-PERF-005: Memory Usage
**Objective**: Monitor memory consumption

**Test Steps**:
1. Run `docker stats spark_pipeline` during processing
2. Process 5000 records
3. Record peak memory usage

**Expected Result**:
- Memory usage remains stable
- No memory leaks
- Peak memory under 2GB

**Pass/Fail**: _____

---

### TC-PERF-006: No Backlog Accumulation
**Objective**: Verify processing keeps up with data generation

**Test Steps**:
1. Run complete pipeline (20 batches)
2. Monitor Spark UI for input vs processing rate
3. Check for backlog

**Expected Result**:
- Processing rate exceeds input rate (10x faster)
- No backlog accumulation
- All files processed within trigger interval

**Pass/Fail**: _____

---

## Test Execution Summary

| Test Suite | Total Tests | Passed | Failed | Not Run |
|------------|-------------|--------|--------|---------|
| CSV File Generation | 5 | | | |
| Spark File Detection | 4 | | | |
| Data Transformation | 10 | | | |
| PostgreSQL Integration | 7 | | | |
| Performance Monitoring | 6 | | | |
| **TOTAL** | **32** | | | |

---

## Test Automation

Automated tests are implemented in `tests/test_pipeline.py` using pytest framework.

**Run all tests**:
```bash
pytest tests/test_pipeline.py -v
```

**Run specific test suite**:
```bash
pytest tests/test_pipeline.py::TestCSVGeneration -v
pytest tests/test_pipeline.py::TestSparkProcessing -v
pytest tests/test_pipeline.py::TestTransformations -v
pytest tests/test_pipeline.py::TestPostgresIntegration -v
pytest tests/test_pipeline.py::TestPerformance -v
```
