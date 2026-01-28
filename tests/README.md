# Test Suite Documentation

## Overview

This directory contains automated tests for the Real-Time E-Commerce Streaming Pipeline. The test suite is organized into multiple test files covering different aspects of the system.

## Test Structure

```
tests/
├── __init__.py                 # Package initialization
├── test_pipeline.py            # Main unit tests for all components
├── test_integration.py         # End-to-end integration tests
├── test_helpers.py             # Helper functions for testing
└── README.md                   # This file
```

## Test Suites

### 1. Unit Tests (test_pipeline.py)

**TestCSVGeneration** - 5 tests
- CSV file creation
- Schema validation
- Data type validation
- Data quality issues
- Batch size validation

**TestSparkProcessing** - 2 tests
- Directory monitoring
- Schema structure

**TestTransformations** - 10 tests
- Timestamp format handling
- Action standardization
- Invalid action handling
- Price correction
- User ID validation
- Product ID validation
- Product name handling
- Session ID handling

**TestPostgresIntegration** - 7 tests
- Database connection
- Table existence
- Table schema
- Action constraints
- Index validation
- Record counting
- Data integrity

**TestPerformance** - 6 tests
- CSV generation speed
- File read performance
- Transformation performance
- Data validation performance
- Memory efficiency
- End-to-end latency

### 2. Integration Tests (test_integration.py)

**TestEndToEndIntegration** - 4 tests
- Full pipeline flow
- Data quality processing
- Performance baseline
- Concurrent file processing

## Prerequisites

Install test dependencies:

```bash
pip install pytest psycopg2-binary pandas numpy python-dotenv
```

## Running Tests

### Run All Unit Tests

```bash
pytest tests/test_pipeline.py -v
```

### Run Specific Test Suite

```bash
# CSV Generation tests
pytest tests/test_pipeline.py::TestCSVGeneration -v

# Transformation tests
pytest tests/test_pipeline.py::TestTransformations -v

# PostgreSQL integration tests
pytest tests/test_pipeline.py::TestPostgresIntegration -v

# Performance tests
pytest tests/test_pipeline.py::TestPerformance -v
```

### Run Specific Test Case

```bash
pytest tests/test_pipeline.py::TestCSVGeneration::test_gen_001_csv_file_creation -v
```

### Run Integration Tests

Integration tests require Docker containers to be running:

```bash
# Start Docker containers first
docker compose up -d

# Set environment variable
export RUN_INTEGRATION_TESTS=true  # Linux/Mac
set RUN_INTEGRATION_TESTS=true     # Windows CMD
$env:RUN_INTEGRATION_TESTS="true"  # Windows PowerShell

# Run integration tests
pytest tests/test_integration.py -v
```

### Run All Tests

```bash
pytest tests/ -v
```

## Test Options

### Verbose Output

```bash
pytest tests/ -v
```

### Show Test Coverage

```bash
pytest tests/ --cov=src --cov-report=html
```

### Run Only Fast Tests

```bash
pytest tests/test_pipeline.py -v -k "not slow"
```

### Stop on First Failure

```bash
pytest tests/ -x
```

### Run Tests in Parallel

```bash
pip install pytest-xdist
pytest tests/ -n 4
```

## Environment Configuration

Set these environment variables before running tests:

```bash
# Database configuration
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=ecommerce_database
export DB_USER=data_user
export DB_PASS=strongpassword

# Enable integration tests
export RUN_INTEGRATION_TESTS=true
```

Or create a `.env` file in the project root:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ecommerce_database
DB_USER=data_user
DB_PASS=strongpassword
RUN_INTEGRATION_TESTS=false
```

## Test Output

### Successful Test Run

```
tests/test_pipeline.py::TestCSVGeneration::test_gen_001_csv_file_creation PASSED
tests/test_pipeline.py::TestCSVGeneration::test_gen_002_csv_schema_validation PASSED
...
========================= 32 passed in 5.23s =========================
```

### Failed Test

```
tests/test_pipeline.py::TestTransformations::test_trans_006_price_correction FAILED

FAILED tests/test_pipeline.py::TestTransformations::test_trans_006_price_correction
AssertionError: All prices should be non-negative after correction
```

## Troubleshooting

### Database Connection Errors

If tests fail with connection errors:

1. Ensure PostgreSQL container is running:
   ```bash
   docker ps | grep postgres
   ```

2. Check database credentials in `.env` file

3. Verify network connectivity:
   ```bash
   psql -h localhost -U data_user -d ecommerce_database
   ```

### Import Errors

If tests fail with import errors:

1. Ensure you're in the project root directory
2. Install all dependencies:
   ```bash
   pip install -r requirements.txt
   pip install pytest
   ```

### Permission Errors

If file permission errors occur:

```bash
# Linux/Mac
chmod -R 755 tests/
chmod -R 755 data/

# Windows
icacls tests /grant Users:F /t
```

## Writing New Tests

### Test Naming Convention

- Test files: `test_*.py`
- Test classes: `Test*`
- Test functions: `test_*`

### Example Test

```python
def test_example_validation():
    """
    Brief description of what this test validates
    """
    # Arrange
    test_data = create_test_data()
    
    # Act
    result = process_data(test_data)
    
    # Assert
    assert result is not None, "Result should not be None"
    assert len(result) > 0, "Result should contain data"
```

## Continuous Integration

Tests can be integrated into CI/CD pipelines:

```yaml
# .github/workflows/tests.yml
name: Run Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.9
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest
      - name: Run unit tests
        run: pytest tests/test_pipeline.py -v
```

## Test Maintenance

- Run tests before committing code
- Update tests when modifying functionality
- Add tests for new features
- Keep test data small and focused
- Document complex test scenarios

## Support

For issues or questions about tests:
1. Check test logs for detailed error messages
2. Review test documentation
3. Verify environment configuration
4. Check database connectivity
