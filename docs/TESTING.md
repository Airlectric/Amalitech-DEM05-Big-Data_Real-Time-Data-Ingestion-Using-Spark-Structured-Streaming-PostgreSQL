# Running Tests in Docker

This guide explains how to run tests for the Real-Time E-Commerce Streaming Pipeline in Docker.

## Prerequisites

- Docker and Docker Compose installed
- `.env` file configured (copy from `.env.example` if needed)

## Quick Start

### Run All Tests

```bash
# On Linux/Mac
bash run_tests.sh all

# On Windows (Git Bash or WSL)
bash run_tests.sh all

# Or using Docker Compose directly
docker-compose --profile test up --abort-on-container-exit
```

### Run Unit Tests Only

Unit tests validate data generation and Spark transformations **without** requiring running services:

```bash
bash run_tests.sh unit

# Or directly
docker-compose run --rm test pytest tests/test_pipeline.py -v
```

**Unit tests include:**
- Data generator function tests (10 tests)
- CSV batch generation tests (5 tests)
- PySpark transformation logic tests (8 tests)
- Schema and configuration tests (3 tests)

**Total: 26 unit tests**

### Run Integration Tests Only

Integration tests require PostgreSQL and Spark services running:

```bash
bash run_tests.sh integration

# Or manually
docker-compose up -d postgres spark
sleep 30  # Wait for services
docker-compose --profile test run --rm test pytest tests/test_integration.py -v
docker-compose down
```

**Integration tests include:**
- Database connection validation
- End-to-end pipeline flow (CSV → Spark → PostgreSQL)
- Data quality processing with actual transformations
- Performance baseline testing
- Concurrent file processing
- Database constraint validation

**Total: 6 integration tests**

## Test Structure

```
tests/
├── conftest.py              # Shared fixtures for both test files
├── test_pipeline.py         # Unit tests (26 tests)
└── test_integration.py      # Integration tests (6 tests)
```

### Shared Fixtures (conftest.py)

- `spark_session` - PySpark session (module scope)
- `clean_test_directory` - Test data cleanup (function scope)
- `db_connection` - PostgreSQL connection (module scope)
- `event_data_dir` - Data directory path
- `db_config` - Database configuration dict

## Docker Test Service

The `docker-compose.yml` includes a `test` service with profile support:

```yaml
test:
  image: dem05/ecommerce-spark:1.0
  container_name: test_runner
  environment:
    RUN_INTEGRATION_TESTS: "true"
  depends_on:
    - postgres
    - spark
  volumes:
    - ./src:/opt/spark/app/src
    - ./tests:/opt/spark/app/tests
    - ./data:/opt/spark/app/data
    - ./lib:/opt/spark/app/lib
    - ./logs/tests:/opt/spark/app/logs/tests
  profiles:
    - test
```

## Test Commands Reference

### Using run_tests.sh Script

```bash
# Run all tests (unit + integration)
bash run_tests.sh all

# Run only unit tests
bash run_tests.sh unit

# Run only integration tests
bash run_tests.sh integration

# View test logs
bash run_tests.sh logs

# Clean up test artifacts
bash run_tests.sh clean
```

### Using Docker Compose Directly

```bash
# Build test image
docker-compose build test

# Run unit tests
docker-compose run --rm test pytest tests/test_pipeline.py -v --tb=short

# Run integration tests (requires services)
docker-compose up -d postgres spark
docker-compose --profile test run --rm test
docker-compose down

# Run specific test
docker-compose run --rm test pytest tests/test_pipeline.py::TestDataGenerator::test_gen_001_generate_fake_event_structure -v

# Run with coverage
docker-compose run --rm test pytest tests/ --cov=src --cov-report=html
```

### Using PowerShell (Windows)

```powershell
# Run all tests
docker-compose --profile test up --abort-on-container-exit; docker-compose down

# Run unit tests only
docker-compose run --rm test pytest tests/test_pipeline.py -v

# View logs
docker-compose logs test
```

## Test Results

Test results are logged to `logs/tests/test_results.log`:

```bash
# View test results
cat logs/tests/test_results.log

# Or use the script
bash run_tests.sh logs
```

## Troubleshooting

### Tests Fail with "Connection Refused"

**Cause:** PostgreSQL or Spark services not ready

**Solution:**
```bash
docker-compose up -d postgres spark
sleep 30  # Wait for services to start
docker-compose --profile test run --rm test
```

### Import Errors

**Cause:** Missing dependencies in Docker image

**Solution:**
```bash
# Rebuild with no cache
docker-compose build --no-cache test
```

### Integration Tests Skipped

**Cause:** `RUN_INTEGRATION_TESTS` not set to "true"

**Solution:**
```bash
# Set in docker-compose.yml test service
environment:
  RUN_INTEGRATION_TESTS: "true"

# Or pass as environment variable
docker-compose run --rm -e RUN_INTEGRATION_TESTS=true test pytest tests/test_integration.py -v
```

### PySpark Session Already Running

**Cause:** Previous test didn't clean up Spark session

**Solution:**
```bash
# Stop all containers
docker-compose down

# Remove test container
docker rm -f test_runner

# Run tests again
bash run_tests.sh all
```

## Continuous Integration

For CI/CD pipelines (GitHub Actions, GitLab CI, etc.):

```yaml
# Example GitHub Actions workflow
test:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v2
    - name: Build Docker images
      run: docker-compose build
    - name: Run unit tests
      run: docker-compose run --rm test pytest tests/test_pipeline.py -v
    - name: Run integration tests
      run: |
        docker-compose up -d postgres spark
        sleep 30
        docker-compose --profile test run --rm test
        docker-compose down
```

## Test Coverage Mapping

Tests are mapped to `docs/test_cases.md`:

| Test Case | Test Implementation | Status |
|-----------|-------------------|--------|
| TC-GEN-001 to TC-GEN-005 | `TestDataGenerator`, `TestDataGeneratorBatch` | ✅ Covered |
| TC-TRANS-001 to TC-TRANS-010 | `TestSparkTransformations` | ✅ Covered |
| TC-DB-001, TC-DB-002, TC-DB-003, TC-DB-006 | `TestEndToEndIntegration` | ✅ Covered |
| TC-SPARK-001 to TC-SPARK-004 | Requires live Spark job | ⚠️ Manual |
| TC-PERF-001 to TC-PERF-006 | Performance monitoring | ⚠️ Manual |

## Additional Notes

- **Unit tests** can run without Docker (just need Python + pytest + pyspark)
- **Integration tests** REQUIRE Docker environment
- Tests use ACTUAL source code from `src/data_generator.py` and `src/spark_streaming_to_postgres.py`
- PySpark transformations match production code exactly
- All fixtures are shared via `conftest.py`

## Questions?

Check the [test_cases.md](docs/test_cases.md) for complete test specifications.
