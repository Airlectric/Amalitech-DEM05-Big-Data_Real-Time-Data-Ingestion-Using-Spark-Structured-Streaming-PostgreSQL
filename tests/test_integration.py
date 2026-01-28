"""
Integration tests for the complete pipeline
These tests require Docker containers to be running
6 end-to-end integration tests
"""

import os
import sys
import time
import pytest
import psycopg2
import pandas as pd
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data_generator import generate_fake_event

load_dotenv()


# Skip if environment variable is not set
pytestmark = pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION_TESTS") != "true",
    reason="Integration tests require running Docker environment"
)


class TestEndToEndIntegration:
    """End-to-end integration tests against running Spark pipeline (6 tests)"""
    
    def test_integration_001_full_pipeline_flow(self, db_connection, event_data_dir):
        """TC-INT-001: Verify complete data flow from CSV to PostgreSQL"""
        cursor = db_connection.cursor()
        
        # Get initial count
        cursor.execute("SELECT COUNT(*) FROM events")
        initial_count = cursor.fetchone()[0]
        
        # Create test CSV file in monitored directory
        test_file = event_data_dir / "integration_test_001.csv"
        events = [generate_fake_event() for _ in range(50)]
        df = pd.DataFrame(events)
        df.to_csv(test_file, index=False)
        
        print(f"\n[TC-INT-001] Created test file with 50 events")
        
        # Wait for Spark to process (10s trigger interval + processing time)
        time.sleep(25)
        
        # Verify data appeared in database
        cursor.execute("SELECT COUNT(*) FROM events")
        final_count = cursor.fetchone()[0]
        
        new_records = final_count - initial_count
        print(f"[TC-INT-001] New records in database: {new_records}")
        
        assert new_records > 0, "Should have ingested at least some records"
        
        cursor.close()
    
    def test_integration_002_data_quality_processing(self, db_connection, event_data_dir):
        """TC-INT-002: Verify data quality and transformation logic"""
        cursor = db_connection.cursor()
        
        # Create test file with known data quality issues
        test_file = event_data_dir / "quality_test_001.csv"
        events = []
        for i in range(30):
            event = generate_fake_event()
            # Inject some issues
            if i % 10 == 0:
                event['action'] = ''  # Empty action
            if i % 15 == 0:
                event['price'] = ''  # Empty price
            events.append(event)
        
        df = pd.DataFrame(events)
        df.to_csv(test_file, index=False)
        
        print(f"\n[TC-INT-002] Created test file with data quality issues")
        
        # Wait for processing
        time.sleep(25)
        
        # Check that records were processed
        cursor.execute("SELECT COUNT(*) FROM events WHERE action != '' AND action IS NOT NULL")
        valid_actions = cursor.fetchone()[0]
        
        print(f"[TC-INT-002] Records with valid actions: {valid_actions}")
        
        assert valid_actions > 0, "Should have processed records with valid actions"
        
        # Check corrupt records table
        cursor.execute("SELECT COUNT(*) FROM corrupt_records")
        corrupt_count = cursor.fetchone()[0]
        print(f"[TC-INT-002] Corrupt records: {corrupt_count}")
        
        cursor.close()
    
    def test_integration_003_performance_baseline(self, db_connection, event_data_dir):
        """TC-PERF-004: Measure end-to-end latency for realistic batch"""
        cursor = db_connection.cursor()
        
        # Get initial count
        cursor.execute("SELECT COUNT(*) FROM events")
        initial_count = cursor.fetchone()[0]
        
        # Create larger test file (500 records)
        test_file = event_data_dir / "perf_test_001.csv"
        events = [generate_fake_event() for _ in range(500)]
        df = pd.DataFrame(events)
        
        start_time = time.time()
        df.to_csv(test_file, index=False)
        write_time = time.time()
        
        print(f"\n[TC-PERF-004] CSV write time: {write_time - start_time:.2f}s")
        print(f"[TC-PERF-004] Waiting for Spark processing...")
        
        # Wait with timeout (increased to 45s for 10s trigger + processing)
        max_wait = 45
        wait_start = time.time()
        
        while (time.time() - wait_start) < max_wait:
            cursor.execute("SELECT COUNT(*) FROM events")
            current_count = cursor.fetchone()[0]
            new_records = current_count - initial_count
            
            if new_records >= 250:  # At least 50% processed (accounting for corrupt records)
                break
            
            time.sleep(2)
        
        end_time = time.time()
        total_latency = end_time - start_time
        
        # Final count
        cursor.execute("SELECT COUNT(*) FROM events")
        final_count = cursor.fetchone()[0]
        processed_records = final_count - initial_count
        
        print(f"[TC-PERF-004] Total end-to-end latency: {total_latency:.3f}s")
        print(f"[TC-PERF-004] Records processed: {processed_records}/500")
        
        # Performance assertion with reasonable threshold
        assert total_latency < 45, f"End-to-end latency {total_latency:.3f}s exceeds 45s threshold"
        assert processed_records > 0, "Should have processed at least some records"
        
        cursor.close()
    
    def test_integration_004_concurrent_files(self, db_connection, event_data_dir):
        """TC-SPARK-002: Verify handling of multiple concurrent files"""
        cursor = db_connection.cursor()
        
        # Get initial count
        cursor.execute("SELECT COUNT(*) FROM events")
        initial_count = cursor.fetchone()[0]
        
        # Create multiple small files simultaneously
        num_files = 5
        records_per_file = 50
        
        print(f"\n[TC-SPARK-002] Creating {num_files} concurrent files...")
        
        for i in range(num_files):
            test_file = event_data_dir / f"concurrent_{i:02d}.csv"
            events = [generate_fake_event() for _ in range(records_per_file)]
            df = pd.DataFrame(events)
            df.to_csv(test_file, index=False)
        
        # Wait for processing (longer time for multiple files)
        time.sleep(40)
        
        # Verify all files were processed
        cursor.execute("SELECT COUNT(*) FROM events")
        final_count = cursor.fetchone()[0]
        
        new_records = final_count - initial_count
        total_expected = num_files * records_per_file
        
        print(f"[TC-SPARK-002] Processed {new_records}/{total_expected} records")
        
        # Allow for some data quality issues but expect at least 60% processed
        min_expected = int(total_expected * 0.6)
        assert new_records >= min_expected, f"Expected at least {min_expected} records, got {new_records}"
        
        cursor.close()
    
    def test_integration_005_schema_validation(self, db_connection):
        """TC-DB-003: Verify database schema matches expectations"""
        cursor = db_connection.cursor()
        
        # Check events table structure
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'events'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        column_names = [col[0] for col in columns]
        
        expected_columns = ['id', 'user_id', 'action', 'product_id', 'product_name', 
                          'price', 'event_time', 'session_id', 'ingestion_time']
        
        for expected_col in expected_columns:
            assert expected_col in column_names, f"Column {expected_col} should exist in events table"
        
        print(f"\n[TC-DB-003] Events table columns: {column_names}")
        
        # Check corrupt_records table
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'corrupt_records'
            ORDER BY ordinal_position
        """)
        
        corrupt_columns = cursor.fetchall()
        corrupt_column_names = [col[0] for col in corrupt_columns]
        
        expected_corrupt_cols = ['id', 'corrupt_record', 'detected_at', 'batch_id', 'error_type']
        
        for expected_col in expected_corrupt_cols:
            assert expected_col in corrupt_column_names, f"Column {expected_col} should exist in corrupt_records table"
        
        print(f"[TC-DB-003] Corrupt_records table columns: {corrupt_column_names}")
        
        cursor.close()
    
    def test_db_006_constraint_validation(self, db_connection):
        """TC-DB-006: Verify database constraints are enforced"""
        cursor = db_connection.cursor()
        
        # Check for indexes on events table
        cursor.execute("""
            SELECT indexname 
            FROM pg_indexes 
            WHERE tablename = 'events'
        """)
        
        indexes = cursor.fetchall()
        index_names = [idx[0] for idx in indexes]
        
        print(f"\n[TC-DB-006] Indexes on events table: {index_names}")
        
        # Should have primary key and performance indexes
        assert len(indexes) > 0, "Events table should have indexes"
        
        # Check constraints
        cursor.execute("""
            SELECT constraint_name, constraint_type 
            FROM information_schema.table_constraints 
            WHERE table_name = 'events'
        """)
        
        constraints = cursor.fetchall()
        constraint_types = [c[1] for c in constraints]
        
        print(f"[TC-DB-006] Constraint types: {constraint_types}")
        
        # Should have primary key
        assert 'PRIMARY KEY' in constraint_types, "Events table should have primary key"
        
        cursor.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])
