"""
Integration tests for the complete pipeline
These tests require Docker containers to be running
"""

import os
import time
import pytest
import psycopg2
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


# Skip if environment variable is not set
pytestmark = pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION_TESTS") != "true",
    reason="Integration tests require running Docker environment"
)


# Test Configuration
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data" / "events"

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "ecommerce_database"),
    "user": os.getenv("DB_USER", "data_user"),
    "password": os.getenv("DB_PASS", "strongpassword")
}


@pytest.fixture(scope="module")
def db_connection():
    """Create database connection for integration tests"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        yield conn
        conn.close()
    except psycopg2.OperationalError as e:
        pytest.skip(f"Cannot connect to database: {e}")


class TestEndToEndIntegration:
    """End-to-end integration tests"""

    def test_integration_001_full_pipeline_flow(self, db_connection):
        """
        Test complete pipeline: CSV generation to database insertion
        """
        # Get initial count
        cursor = db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM events;")
        initial_count = cursor.fetchone()[0]
        
        # Create test CSV
        test_data = {
            'user_id': [100, 101, 102],
            'action': ['view', 'purchase', 'add_to_cart'],
            'product_id': [1001, 1002, 1003],
            'product_name': ['Laptop', 'Phone', 'Tablet'],
            'price': [999.99, 599.99, 399.99],
            'timestamp': [datetime.now().isoformat()] * 3,
            'session_id': ['12345', '12346', '12347']
        }
        
        df = pd.DataFrame(test_data)
        test_file = DATA_DIR / f"integration_test_{int(time.time())}.csv"
        df.to_csv(test_file, index=False)
        
        # Wait for Spark to process (trigger interval + processing time)
        time.sleep(15)
        
        # Check new count
        cursor.execute("SELECT COUNT(*) FROM events;")
        new_count = cursor.fetchone()[0]
        cursor.close()
        
        # Should have at least the 3 new records
        assert new_count >= initial_count + 3, "New records should be in database"

    def test_integration_002_data_quality_processing(self, db_connection):
        """
        Test that data quality issues are handled correctly
        """
        # Create CSV with quality issues
        test_data = {
            'user_id': [None, 100, -5],
            'action': ['', 'VIEW', 'invalid'],
            'product_id': [-1, 1001, 0],
            'product_name': ['', 'Laptop', 'Phone'],
            'price': [-10.0, 999.99, -50.0],
            'timestamp': ['INVALID', datetime.now().isoformat(), ''],
            'session_id': [None, '12345', '']
        }
        
        df = pd.DataFrame(test_data)
        test_file = DATA_DIR / f"quality_test_{int(time.time())}.csv"
        df.to_csv(test_file, index=False)
        
        # Wait for processing
        time.sleep(15)
        
        # Query the processed data
        cursor = db_connection.cursor()
        cursor.execute("""
            SELECT action, price, product_name, session_id
            FROM events
            ORDER BY ingestion_time DESC
            LIMIT 3;
        """)
        results = cursor.fetchall()
        cursor.close()
        
        # Verify transformations were applied
        for row in results:
            action, price, product_name, session_id = row
            
            # All actions should be valid
            assert action in ['VIEW', 'PURCHASE', 'ADD_TO_CART', 'REMOVE_FROM_CART', 'UNKNOWN']
            
            # No negative prices
            assert price >= 0
            
            # No empty product names
            assert product_name != ''
            
            # No empty session IDs
            assert session_id != ''

    def test_integration_003_performance_baseline(self, db_connection):
        """
        Test performance meets baseline requirements
        """
        num_records = 500
        
        # Generate batch
        test_data = {
            'user_id': list(range(num_records)),
            'action': ['view'] * num_records,
            'product_id': list(range(1001, 1001 + num_records)),
            'product_name': ['Laptop'] * num_records,
            'price': [999.99] * num_records,
            'timestamp': [datetime.now().isoformat()] * num_records,
            'session_id': [str(10000 + i) for i in range(num_records)]
        }
        
        df = pd.DataFrame(test_data)
        test_file = DATA_DIR / f"perf_test_{int(time.time())}.csv"
        
        # Get initial count and timestamp
        cursor = db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM events;")
        initial_count = cursor.fetchone()[0]
        
        # Write file and record time
        file_create_time = time.time()
        df.to_csv(test_file, index=False)
        
        # Wait for processing (max 30 seconds)
        max_wait = 30
        start_wait = time.time()
        
        while time.time() - start_wait < max_wait:
            cursor.execute("SELECT COUNT(*) FROM events;")
            current_count = cursor.fetchone()[0]
            
            if current_count >= initial_count + num_records:
                break
            
            time.sleep(2)
        
        processing_complete_time = time.time()
        cursor.close()
        
        # Calculate latency
        end_to_end_latency = processing_complete_time - file_create_time
        
        # Should complete within reasonable time
        assert end_to_end_latency < 30, f"Processing took {end_to_end_latency}s, should be under 30s"

    def test_integration_004_concurrent_files(self, db_connection):
        """
        Test processing multiple files concurrently
        """
        num_files = 5
        records_per_file = 50
        
        cursor = db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM events;")
        initial_count = cursor.fetchone()[0]
        
        # Create multiple files
        for i in range(num_files):
            test_data = {
                'user_id': list(range(i * records_per_file, (i + 1) * records_per_file)),
                'action': ['view'] * records_per_file,
                'product_id': list(range(1001, 1001 + records_per_file)),
                'product_name': ['Laptop'] * records_per_file,
                'price': [999.99] * records_per_file,
                'timestamp': [datetime.now().isoformat()] * records_per_file,
                'session_id': [str(10000 + j) for j in range(records_per_file)]
            }
            
            df = pd.DataFrame(test_data)
            test_file = DATA_DIR / f"concurrent_{i}_{int(time.time())}.csv"
            df.to_csv(test_file, index=False)
        
        # Wait for all files to be processed
        time.sleep(20)
        
        cursor.execute("SELECT COUNT(*) FROM events;")
        new_count = cursor.fetchone()[0]
        cursor.close()
        
        expected_new_records = num_files * records_per_file
        assert new_count >= initial_count + expected_new_records, \
            f"Expected at least {expected_new_records} new records"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
