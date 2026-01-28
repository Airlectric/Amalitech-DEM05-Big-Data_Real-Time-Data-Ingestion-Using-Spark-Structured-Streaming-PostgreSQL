"""
Integration tests for the complete pipeline
These tests require Docker containers to be running
Uses ACTUAL source code functions from data_generator.py
"""

import os
import sys
import time
import pytest
import pandas as pd
from pathlib import Path
from threading import Thread
from pyspark.sql.functions import col, to_timestamp, when, coalesce, trim, upper, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Import ACTUAL source code functions
from data_generator import generate_fake_event


# Skip if environment variable is not set
pytestmark = pytest.mark.skipif(
    os.getenv("RUN_INTEGRATION_TESTS") != "true",
    reason="Integration tests require running Docker environment"
)


class TestEndToEndIntegration:
    """End-to-end integration tests using ACTUAL source code"""

    def test_db_001_database_connection(self, db_connection, db_config):
        """
        TC-DB-001: Verify database connection is established
        Tests ACTUAL PostgreSQL connection with configuration
        """
        # Verify connection is active
        assert db_connection is not None, "Database connection should exist"
        
        # Verify we can execute queries
        cursor = db_connection.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        assert version is not None, "Should retrieve PostgreSQL version"
        
        # Verify events table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'events'
            );
        """)
        table_exists = cursor.fetchone()[0]
        assert table_exists, "Events table should exist"
        cursor.close()

    def test_integration_001_full_pipeline_flow(self, db_connection, event_data_dir):
        """
        Test complete pipeline: CSV generation to database insertion
        Uses ACTUAL generate_fake_event function
        """
        # Get initial count
        cursor = db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM events;")
        initial_count = cursor.fetchone()[0]
        
        # Generate events using ACTUAL source code function
        events = [generate_fake_event(batch_num=0) for _ in range(3)]
        df = pd.DataFrame(events)
        
        # Write CSV file
        test_file = event_data_dir / f"integration_test_{int(time.time())}.csv"
        df.to_csv(test_file, index=False)
        
        # Wait for Spark to process (trigger interval + processing time)
        time.sleep(15)
        
        # Check new count
        cursor.execute("SELECT COUNT(*) FROM events;")
        new_count = cursor.fetchone()[0]
        cursor.close()
        
        # Should have at least the 3 new records
        assert new_count >= initial_count + 3, "New records should be in database"

    def test_integration_002_data_quality_processing(self, db_connection, event_data_dir, spark_session):
        """
        Test that data quality issues from ACTUAL generate_fake_event are handled correctly
        Uses ACTUAL Spark transformations to verify data cleaning
        """
        # Generate events with ACTUAL function (already has quality issues built in)
        events = [generate_fake_event(batch_num=0) for _ in range(100)]
        df_pandas = pd.DataFrame(events)
        
        # Test transformations locally with actual PySpark before writing to file
        schema = StructType([
            StructField("user_id", LongType(), True),
            StructField("action", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("product_name", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("session_id", StringType(), True)
        ])
        
        # Convert to Spark DataFrame and apply ACTUAL transformations
        df_spark = spark_session.createDataFrame(events, schema)
        cleaned_df = (
            df_spark
            .withColumn(
                "event_time",
                coalesce(
                    to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
                    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
                    to_timestamp(lit(None), "yyyy-MM-dd HH:mm:ss")
                )
            )
            .withColumn("event_time", coalesce(col("event_time"), current_timestamp()))
            .withColumn("action", upper(trim(coalesce(col("action"), lit("UNKNOWN")))))
            .withColumn(
                "action",
                when(col("action").isin("VIEW", "PURCHASE", "ADD_TO_CART", "REMOVE_FROM_CART"), col("action"))
                .otherwise("UNKNOWN")
            )
            .withColumn("price", when(col("price") >= 0, col("price")).otherwise(0.0))
            .withColumn("user_id", when(col("user_id").isNotNull() & (col("user_id") > 0), col("user_id")))
            .withColumn("product_id", when(col("product_id") > 0, col("product_id")))
            .withColumn("product_name", trim(coalesce(col("product_name"), lit("Unknown Product"))))
            .withColumn("session_id", coalesce(col("session_id"), lit("unknown")))
        )
        
        # Verify transformations locally
        results = cleaned_df.collect()
        for row in results:
            assert row["action"] in ['VIEW', 'PURCHASE', 'ADD_TO_CART', 'REMOVE_FROM_CART', 'UNKNOWN'], \
                f"Action should be standardized, got {row['action']}"
            assert row["price"] >= 0, f"Price should be non-negative, got {row['price']}"
            assert row["event_time"] is not None, "event_time should never be null"
        
        # Write CSV for end-to-end test
        test_file = event_data_dir / f"quality_test_{int(time.time())}.csv"
        df_pandas.to_csv(test_file, index=False)
        
        # Wait for processing
        time.sleep(15)
        
        # Query the processed data
        cursor = db_connection.cursor()
        cursor.execute("""
            SELECT action, price, product_name, session_id
            FROM events
            ORDER BY ingestion_time DESC
            LIMIT 10;
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

    def test_integration_003_performance_baseline(self, db_connection, event_data_dir):
        """
        Test performance meets baseline requirements
        Uses ACTUAL generate_fake_event function
        """
        num_records = 500
        
        # Generate events using ACTUAL source code function
        events = [generate_fake_event(batch_num=0) for _ in range(num_records)]
        df = pd.DataFrame(events)
        
        test_file = event_data_dir / f"perf_test_{int(time.time())}.csv"
        
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

    def test_integration_004_concurrent_files(self, db_connection, event_data_dir):
        """
        Test processing multiple files concurrently
        Uses ACTUAL generate_fake_event function
        """
        num_files = 5
        records_per_file = 50
        
        cursor = db_connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM events;")
        initial_count = cursor.fetchone()[0]

        def create_csv_file(file_index, timestamp, data_dir):
            """Helper to create CSV using actual generate_fake_event"""
            events = [generate_fake_event(batch_num=file_index) for _ in range(records_per_file)]
            df = pd.DataFrame(events)
            file_path = data_dir / f"concurrent_{file_index}_{timestamp}.csv"
            df.to_csv(file_path, index=False)

        timestamp = int(time.time())
        threads = []
        for i in range(num_files):
            t = Thread(target=create_csv_file, args=(i, timestamp, event_data_dir))
            t.start()
            threads.append(t)

        # Wait for all threads to finish
        for t in threads:
            t.join()
        
        # Wait for all files to be processed
        time.sleep(20)
        
        cursor.execute("SELECT COUNT(*) FROM events;")
        new_count = cursor.fetchone()[0]
        cursor.close()
        
        expected_new_records = num_files * records_per_file
        assert new_count >= initial_count + expected_new_records, \
            f"Expected at least {expected_new_records} new records"

    def test_db_006_constraint_validation(self, db_connection):
        """
        TC-DB-006: Verify database constraints are enforced
        Tests that only valid actions are stored in database
        """
        cursor = db_connection.cursor()
        
        # Query all distinct actions from database
        cursor.execute("SELECT DISTINCT action FROM events;")
        actions = [row[0] for row in cursor.fetchall()]
        
        # Verify all actions comply with CHECK constraint
        valid_actions = {'VIEW', 'PURCHASE', 'ADD_TO_CART', 'REMOVE_FROM_CART', 'UNKNOWN'}
        for action in actions:
            assert action in valid_actions, \
                f"Invalid action '{action}' found in database. Should be one of {valid_actions}"
        
        # Verify no null actions exist
        cursor.execute("SELECT COUNT(*) FROM events WHERE action IS NULL;")
        null_count = cursor.fetchone()[0]
        assert null_count == 0, "Should have no null actions in database"
        
        cursor.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
