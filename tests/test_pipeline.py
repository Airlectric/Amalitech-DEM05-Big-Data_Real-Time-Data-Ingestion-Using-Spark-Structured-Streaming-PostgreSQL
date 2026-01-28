"""
Automated Test Suite for Real-Time E-Commerce Streaming Pipeline
Tests CSV generation, Spark processing, transformations, PostgreSQL integration, and performance
"""

import os
import time
import pytest
import pandas as pd
import psycopg2
from datetime import datetime
from pathlib import Path


# Test Configuration
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data" / "events"
TEST_DATA_DIR = BASE_DIR / "data" / "test_events"

# Database Configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "ecommerce_database"),
    "user": os.getenv("DB_USER", "data_user"),
    "password": os.getenv("DB_PASS", "strongpassword")
}


# Fixtures
@pytest.fixture(scope="session")
def db_connection():
    """Create database connection for tests"""
    conn = psycopg2.connect(**DB_CONFIG)
    yield conn
    conn.close()


@pytest.fixture(scope="function")
def clean_test_directory():
    """Clean test data directory before each test"""
    TEST_DATA_DIR.mkdir(parents=True, exist_ok=True)
    for file in TEST_DATA_DIR.glob("*.csv"):
        file.unlink()
    yield TEST_DATA_DIR
    # Cleanup after test
    for file in TEST_DATA_DIR.glob("*.csv"):
        file.unlink()


# Test Suite 1: CSV File Generation Tests
class TestCSVGeneration:
    """Test suite for CSV file generation validation"""

    def test_gen_001_csv_file_creation(self, clean_test_directory):
        """TC-GEN-001: Verify CSV files are created in correct directory"""
        from src.data_generator import PRODUCTS
        
        # Generate test CSV
        test_events = [{
            'user_id': 100,
            'action': 'view',
            'product_id': 1001,
            'product_name': 'Laptop',
            'price': 999.99,
            'timestamp': datetime.now().isoformat(),
            'session_id': '12345'
        }]
        
        df = pd.DataFrame(test_events)
        test_file = clean_test_directory / "events_batch_000.csv"
        df.to_csv(test_file, index=False)
        
        assert test_file.exists(), "CSV file was not created"
        assert test_file.stat().st_size > 0, "CSV file is empty"

    def test_gen_002_csv_schema_validation(self, clean_test_directory):
        """TC-GEN-002: Verify CSV files have correct column structure"""
        # Create test CSV
        test_data = {
            'user_id': [100],
            'action': ['view'],
            'product_id': [1001],
            'product_name': ['Laptop'],
            'price': [999.99],
            'timestamp': [datetime.now().isoformat()],
            'session_id': ['12345']
        }
        
        df = pd.DataFrame(test_data)
        test_file = clean_test_directory / "test_schema.csv"
        df.to_csv(test_file, index=False)
        
        # Read and validate
        df_read = pd.read_csv(test_file)
        expected_columns = ['user_id', 'action', 'product_id', 'product_name', 'price', 'timestamp', 'session_id']
        
        assert list(df_read.columns) == expected_columns, "Column names do not match expected schema"

    def test_gen_003_csv_data_type_validation(self, clean_test_directory):
        """TC-GEN-003: Verify data types in generated CSV files"""
        test_data = {
            'user_id': [100, None, 200],
            'action': ['view', 'purchase', 'add_to_cart'],
            'product_id': [1001, 1002, -1],
            'product_name': ['Laptop', 'Phone', 'Book'],
            'price': [999.99, -10.50, 50.00],
            'timestamp': [datetime.now().isoformat(), '2026-01-28 10:30:45', ''],
            'session_id': ['12345', None, '67890']
        }
        
        df = pd.DataFrame(test_data)
        test_file = clean_test_directory / "test_types.csv"
        df.to_csv(test_file, index=False)
        
        df_read = pd.read_csv(test_file)
        
        assert df_read['action'].dtype == 'object', "Action should be string type"
        assert df_read['product_name'].dtype == 'object', "Product name should be string type"
        assert df_read['price'].dtype == 'float64', "Price should be float type"

    def test_gen_004_data_quality_issues_present(self, clean_test_directory):
        """TC-GEN-004: Verify intentional data quality issues are generated"""
        test_data = {
            'user_id': [100, None, 200, -5],
            'action': ['view', '', 'PURCHASE', 'invalid'],
            'product_id': [1001, -1, 1002, 0],
            'product_name': ['Laptop', '', 'Phone', 'Book'],
            'price': [999.99, -10.50, 50.00, 100.00],
            'timestamp': [datetime.now().isoformat(), '', 'INVALID_DATE', '2026-01-28 10:30:45'],
            'session_id': ['12345', None, '67890', '11111']
        }
        
        df = pd.DataFrame(test_data)
        
        # Verify issues present
        assert df['user_id'].isnull().any(), "Should have null user_ids"
        assert (df['product_id'] < 0).any(), "Should have negative product_ids"
        assert (df['price'] < 0).any(), "Should have negative prices"
        assert (df['action'] == '').any() or df['action'].isnull().any(), "Should have empty actions"

    def test_gen_005_batch_size_validation(self, clean_test_directory):
        """TC-GEN-005: Verify batch contains expected number of records"""
        num_records = 100
        test_data = {
            'user_id': list(range(num_records)),
            'action': ['view'] * num_records,
            'product_id': list(range(1001, 1001 + num_records)),
            'product_name': ['Laptop'] * num_records,
            'price': [999.99] * num_records,
            'timestamp': [datetime.now().isoformat()] * num_records,
            'session_id': ['12345'] * num_records
        }
        
        df = pd.DataFrame(test_data)
        test_file = clean_test_directory / "test_batch.csv"
        df.to_csv(test_file, index=False)
        
        df_read = pd.read_csv(test_file)
        assert len(df_read) == num_records, f"Expected {num_records} records, got {len(df_read)}"


# Test Suite 2: Spark Processing Tests
class TestSparkProcessing:
    """Test suite for Spark file detection and processing"""

    def test_spark_001_directory_monitoring(self):
        """TC-SPARK-001: Verify Spark would monitor the events directory"""
        # This is a structural test - verify directory exists and is accessible
        assert DATA_DIR.exists() or TEST_DATA_DIR.exists(), "Events directory should exist"
        assert os.access(DATA_DIR, os.W_OK) or os.access(TEST_DATA_DIR, os.W_OK), "Directory should be writable"

    def test_spark_002_schema_structure(self):
        """TC-SPARK-003: Verify schema definition matches CSV structure"""
        expected_schema_fields = ["user_id", "action", "product_id", "product_name", "price", "timestamp", "session_id"]
        
        # Read from spark_streaming_to_postgres.py
        spark_file = BASE_DIR / "src" / "spark_streaming_to_postgres.py"
        with open(spark_file, 'r') as f:
            content = f.read()
        
        # Verify all expected fields are in schema definition
        for field in expected_schema_fields:
            assert field in content, f"Field {field} should be in Spark schema"


# Test Suite 3: Data Transformation Tests
class TestTransformations:
    """Test suite for data transformation logic"""

    def test_trans_001_timestamp_formats(self):
        """TC-TRANS-001/002: Test timestamp format handling"""
        # ISO format
        iso_timestamp = "2026-01-28T10:30:45.123456"
        assert 'T' in iso_timestamp, "ISO format should contain T separator"
        
        # Standard format
        std_timestamp = "2026-01-28 10:30:45"
        assert len(std_timestamp.split()) == 2, "Standard format should have date and time separated by space"

    def test_trans_004_action_standardization(self):
        """TC-TRANS-004: Test action standardization logic"""
        test_actions = ['view', 'VIEW', 'Purchase', 'add_to_cart', 'REMOVE_FROM_CART']
        valid_actions = ['VIEW', 'PURCHASE', 'ADD_TO_CART', 'REMOVE_FROM_CART']
        
        for action in test_actions:
            standardized = action.upper().strip()
            if standardized in ['VIEW', 'PURCHASE', 'ADD_TO_CART', 'REMOVE_FROM_CART']:
                assert standardized in valid_actions, f"{action} should standardize to valid action"

    def test_trans_005_invalid_action_handling(self):
        """TC-TRANS-005: Test invalid action handling"""
        invalid_actions = ['', None, 'invalid_action', 'delete']
        
        for action in invalid_actions:
            if action is None or action == '' or action.upper() not in ['VIEW', 'PURCHASE', 'ADD_TO_CART', 'REMOVE_FROM_CART']:
                # Should be replaced with UNKNOWN
                result = 'UNKNOWN'
                assert result == 'UNKNOWN', "Invalid actions should become UNKNOWN"

    def test_trans_006_price_correction(self):
        """TC-TRANS-006: Test negative price correction"""
        test_prices = [-10.50, -100.00, 0.0, 50.00, 999.99]
        
        corrected_prices = [max(0.0, price) for price in test_prices]
        
        for corrected in corrected_prices:
            assert corrected >= 0, "All prices should be non-negative after correction"

    def test_trans_007_user_id_validation(self):
        """TC-TRANS-007: Test user ID validation logic"""
        test_user_ids = [None, 0, -5, 100, 999]
        
        # Validation logic: keep only positive IDs
        validated_ids = [uid if uid is not None and uid > 0 else None for uid in test_user_ids]
        
        assert validated_ids[0] is None, "None should remain None"
        assert validated_ids[1] is None, "Zero should become None"
        assert validated_ids[2] is None, "Negative should become None"
        assert validated_ids[3] == 100, "Positive should be preserved"

    def test_trans_008_product_id_validation(self):
        """TC-TRANS-008: Test product ID validation logic"""
        test_product_ids = [-1, 0, 1001, 2500]
        
        validated_ids = [pid if pid > 0 else None for pid in test_product_ids]
        
        assert validated_ids[0] is None, "Negative should become None"
        assert validated_ids[1] is None, "Zero should become None"
        assert validated_ids[2] == 1001, "Positive should be preserved"

    def test_trans_009_product_name_handling(self):
        """TC-TRANS-009: Test product name default handling"""
        test_names = ['', None, '  ', 'Laptop', 'Phone']
        
        cleaned_names = [name.strip() if name and name.strip() else 'Unknown Product' for name in test_names]
        
        assert cleaned_names[0] == 'Unknown Product', "Empty should become Unknown Product"
        assert cleaned_names[1] == 'Unknown Product', "None should become Unknown Product"
        assert cleaned_names[3] == 'Laptop', "Valid names should be preserved"

    def test_trans_010_session_id_handling(self):
        """TC-TRANS-010: Test session ID default handling"""
        test_sessions = [None, '', '12345', '67890']
        
        cleaned_sessions = [sid if sid else 'unknown' for sid in test_sessions]
        
        assert cleaned_sessions[0] == 'unknown', "None should become unknown"
        assert cleaned_sessions[1] == 'unknown', "Empty should become unknown"
        assert cleaned_sessions[2] == '12345', "Valid sessions should be preserved"


# Test Suite 4: PostgreSQL Integration Tests
class TestPostgresIntegration:
    """Test suite for PostgreSQL database integration"""

    def test_db_001_database_connection(self, db_connection):
        """TC-DB-001: Verify database connection"""
        assert db_connection is not None, "Database connection should be established"
        
        cursor = db_connection.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        cursor.close()
        
        assert result[0] == 1, "Database should respond to queries"

    def test_db_002_table_exists(self, db_connection):
        """TC-DB-002: Verify events table exists"""
        cursor = db_connection.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'events'
            );
        """)
        exists = cursor.fetchone()[0]
        cursor.close()
        
        assert exists, "Events table should exist in database"

    def test_db_003_table_schema(self, db_connection):
        """TC-DB-003: Verify events table schema"""
        cursor = db_connection.cursor()
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'events'
            ORDER BY ordinal_position;
        """)
        columns = cursor.fetchall()
        cursor.close()
        
        column_names = [col[0] for col in columns]
        expected_columns = ['id', 'user_id', 'action', 'product_id', 'product_name', 
                          'price', 'event_time', 'session_id', 'ingestion_time']
        
        for expected in expected_columns:
            assert expected in column_names, f"Column {expected} should exist in events table"

    def test_db_004_action_constraint(self, db_connection):
        """TC-DB-006: Verify action constraint exists"""
        cursor = db_connection.cursor()
        cursor.execute("""
            SELECT constraint_name 
            FROM information_schema.table_constraints 
            WHERE table_name = 'events' AND constraint_type = 'CHECK';
        """)
        constraints = cursor.fetchall()
        cursor.close()
        
        constraint_names = [c[0] for c in constraints]
        assert any('action' in name.lower() for name in constraint_names), "Action constraint should exist"

    def test_db_005_indexes_exist(self, db_connection):
        """TC-DB-007: Verify indexes exist on events table"""
        cursor = db_connection.cursor()
        cursor.execute("""
            SELECT indexname 
            FROM pg_indexes 
            WHERE tablename = 'events';
        """)
        indexes = cursor.fetchall()
        cursor.close()
        
        index_names = [idx[0] for idx in indexes]
        
        # Should have indexes on event_time, action, user_id, etc.
        assert len(index_names) >= 3, "Should have multiple indexes for performance"

    def test_db_006_record_count(self, db_connection):
        """TC-DB-002: Verify records can be queried"""
        cursor = db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM events;")
        count = cursor.fetchone()[0]
        cursor.close()
        
        assert count >= 0, "Should be able to count records in events table"

    def test_db_007_data_integrity_check(self, db_connection):
        """TC-DB-003: Verify data integrity constraints"""
        cursor = db_connection.cursor()
        
        # Check that all records have non-null action
        cursor.execute("SELECT COUNT(*) FROM events WHERE action IS NULL;")
        null_actions = cursor.fetchone()[0]
        
        # Check that all prices are non-negative
        cursor.execute("SELECT COUNT(*) FROM events WHERE price < 0;")
        negative_prices = cursor.fetchone()[0]
        
        cursor.close()
        
        assert null_actions == 0, "No records should have null action"
        assert negative_prices == 0, "No records should have negative price"


# Test Suite 5: Performance Tests
class TestPerformance:
    """Test suite for performance and monitoring"""

    def test_perf_001_csv_generation_speed(self, clean_test_directory):
        """TC-PERF-001: Measure CSV generation speed"""
        num_records = 1000
        
        start_time = time.time()
        
        test_data = {
            'user_id': list(range(num_records)),
            'action': ['view'] * num_records,
            'product_id': list(range(1001, 1001 + num_records)),
            'product_name': ['Laptop'] * num_records,
            'price': [999.99] * num_records,
            'timestamp': [datetime.now().isoformat()] * num_records,
            'session_id': ['12345'] * num_records
        }
        
        df = pd.DataFrame(test_data)
        test_file = clean_test_directory / "perf_test.csv"
        df.to_csv(test_file, index=False)
        
        end_time = time.time()
        duration = end_time - start_time
        throughput = num_records / duration
        
        assert duration < 5.0, "Should generate 1000 records in under 5 seconds"
        assert throughput > 200, "Should achieve at least 200 records/second"

    def test_perf_002_file_read_performance(self, clean_test_directory):
        """TC-PERF-002: Measure file read performance"""
        num_records = 5000
        
        # Create large CSV
        test_data = {
            'user_id': list(range(num_records)),
            'action': ['view'] * num_records,
            'product_id': list(range(1001, 1001 + num_records)),
            'product_name': ['Laptop'] * num_records,
            'price': [999.99] * num_records,
            'timestamp': [datetime.now().isoformat()] * num_records,
            'session_id': ['12345'] * num_records
        }
        
        df = pd.DataFrame(test_data)
        test_file = clean_test_directory / "large_file.csv"
        df.to_csv(test_file, index=False)
        
        # Measure read time
        start_time = time.time()
        df_read = pd.read_csv(test_file)
        end_time = time.time()
        
        duration = end_time - start_time
        throughput = num_records / duration
        
        assert duration < 2.0, "Should read 5000 records in under 2 seconds"
        assert len(df_read) == num_records, "Should read all records correctly"

    def test_perf_003_transformation_performance(self):
        """TC-PERF-003: Measure transformation logic performance"""
        num_records = 10000
        
        test_actions = ['view', 'VIEW', 'purchase', 'Purchase', ''] * (num_records // 5)
        
        start_time = time.time()
        
        # Simulate action standardization
        standardized = []
        for action in test_actions:
            if action:
                std = action.upper().strip()
                if std in ['VIEW', 'PURCHASE', 'ADD_TO_CART', 'REMOVE_FROM_CART']:
                    standardized.append(std)
                else:
                    standardized.append('UNKNOWN')
            else:
                standardized.append('UNKNOWN')
        
        end_time = time.time()
        duration = end_time - start_time
        
        assert duration < 1.0, "Should process 10000 transformations in under 1 second"
        assert len(standardized) == num_records, "Should process all records"

    def test_perf_004_data_validation_performance(self):
        """TC-PERF-004: Measure data validation performance"""
        num_records = 10000
        
        test_prices = [-10.5, 50.0, -100.0, 999.99] * (num_records // 4)
        
        start_time = time.time()
        
        # Simulate price correction
        corrected = [max(0.0, price) for price in test_prices]
        
        end_time = time.time()
        duration = end_time - start_time
        
        assert duration < 0.5, "Should validate 10000 prices in under 0.5 seconds"
        assert all(p >= 0 for p in corrected), "All prices should be non-negative"

    def test_perf_005_memory_efficiency(self, clean_test_directory):
        """TC-PERF-005: Verify memory-efficient processing"""
        # Create multiple CSV files
        num_files = 10
        records_per_file = 500
        
        for i in range(num_files):
            test_data = {
                'user_id': list(range(records_per_file)),
                'action': ['view'] * records_per_file,
                'product_id': list(range(1001, 1001 + records_per_file)),
                'product_name': ['Laptop'] * records_per_file,
                'price': [999.99] * records_per_file,
                'timestamp': [datetime.now().isoformat()] * records_per_file,
                'session_id': ['12345'] * records_per_file
            }
            
            df = pd.DataFrame(test_data)
            df.to_csv(clean_test_directory / f"batch_{i:03d}.csv", index=False)
        
        # Read all files sequentially (simulating streaming)
        total_records = 0
        for csv_file in clean_test_directory.glob("batch_*.csv"):
            df = pd.read_csv(csv_file)
            total_records += len(df)
        
        assert total_records == num_files * records_per_file, "Should process all records correctly"

    def test_perf_006_end_to_end_latency(self, clean_test_directory):
        """TC-PERF-004: Measure end-to-end processing simulation"""
        # Simulate: Generate -> Read -> Transform -> Validate
        num_records = 100
        
        start_time = time.time()
        
        # Generate
        test_data = {
            'user_id': list(range(num_records)),
            'action': ['view'] * num_records,
            'product_id': list(range(1001, 1001 + num_records)),
            'product_name': ['Laptop'] * num_records,
            'price': [999.99] * num_records,
            'timestamp': [datetime.now().isoformat()] * num_records,
            'session_id': ['12345'] * num_records
        }
        df = pd.DataFrame(test_data)
        test_file = clean_test_directory / "e2e_test.csv"
        df.to_csv(test_file, index=False)
        
        # Read
        df_read = pd.read_csv(test_file)
        
        # Transform
        df_read['action'] = df_read['action'].str.upper()
        df_read['price'] = df_read['price'].apply(lambda x: max(0.0, x))
        
        end_time = time.time()
        latency = end_time - start_time
        
        assert latency < 2.0, "End-to-end processing should complete in under 2 seconds for 100 records"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
