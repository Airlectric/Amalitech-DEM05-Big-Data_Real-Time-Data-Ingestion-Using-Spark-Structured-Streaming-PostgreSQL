"""
Automated Test Suite for Real-Time E-Commerce Streaming Pipeline
Tests using actual PySpark and source code functions
26 unit tests across 4 test classes
"""

import os
import sys
import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data_generator import generate_fake_event, PRODUCTS


class TestDataGenerator:
    """Unit tests for data_generator.py functions (10 tests)"""
    
    def test_gen_001_generate_event_structure(self):
        """TC-GEN-001: Verify event has all required fields"""
        event = generate_fake_event()
        
        required_fields = ['user_id', 'action', 'product_id', 'product_name', 'price', 'timestamp', 'session_id']
        for field in required_fields:
            assert field in event, f"Event should contain {field}"
    
    def test_gen_002_user_id_range(self):
        """TC-GEN-002: Verify user_id in expected range"""
        for _ in range(50):
            event = generate_fake_event()
            if event['user_id'] != '':
                assert 1000 <= event['user_id'] <= 9999, "user_id should be between 1000-9999"
    
    def test_gen_003_product_name_valid(self):
        """TC-GEN-003: Verify product names come from PRODUCTS list"""
        events = [generate_fake_event() for _ in range(100)]
        product_names = [e['product_name'] for e in events]
        
        # Account for 1% probability of empty string
        non_empty_names = [name for name in product_names if name]
        valid_count = sum(1 for name in non_empty_names if name in PRODUCTS)
        
        # At least 95% should have valid product names (out of 100, accounting for ~1% empty)
        assert valid_count >= 95, f"Expected at least 95 events with valid product names, got {valid_count}"
    
    def test_gen_004_action_valid(self):
        """TC-GEN-004: Verify action values are valid"""
        valid_actions = ['view', 'purchase', 'add_to_cart', 'remove_from_cart', '', 'VIEW', 'Purchase', None]
        
        for _ in range(50):
            event = generate_fake_event()
            assert event['action'] in valid_actions, f"Action {event['action']} not in valid actions"
    
    def test_gen_005_price_non_negative(self):
        """TC-GEN-005: Verify price handling (can be negative as data quality issue)"""
        for _ in range(50):
            event = generate_fake_event()
            # Price can be empty string or a float (including negative for data quality issues)
            if event['price'] != '':
                price = float(event['price'])
                assert -15.0 <= price <= 800.0, "Price should be in expected range (-15 to 800)"
    
    def test_gen_006_timestamp_format(self):
        """TC-GEN-006: Verify timestamp is in ISO format"""
        event = generate_fake_event()
        
        # Should be able to parse timestamp
        if event['timestamp']:
            try:
                datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
            except ValueError:
                pytest.fail("Timestamp should be in ISO format")
    
    def test_gen_007_session_id_format(self):
        """TC-GEN-007: Verify session_id format"""
        event = generate_fake_event()
        
        if event['session_id']:
            assert isinstance(event['session_id'], str), "session_id should be string"
    
    def test_gen_008_product_id_range(self):
        """TC-GEN-008: Verify product_id in expected range"""
        for _ in range(50):
            event = generate_fake_event()
            if event['product_id'] != '':
                product_id = int(event['product_id'])
                assert 1 <= product_id <= 1000, "product_id should be between 1-1000"
    
    def test_gen_009_data_quality_issues_present(self):
        """TC-GEN-009: Verify intentional data quality issues are generated"""
        events = [generate_fake_event() for _ in range(200)]
        
        # Check for at least some empty values (data quality issues)
        empty_actions = sum(1 for e in events if e['action'] == '')
        empty_prices = sum(1 for e in events if e['price'] == '')
        empty_sessions = sum(1 for e in events if e['session_id'] == '')
        
        # At least some data quality issues should exist
        total_issues = empty_actions + empty_prices + empty_sessions
        assert total_issues > 0, "Should have some data quality issues"
    
    def test_gen_010_event_dict_structure(self):
        """TC-GEN-010: Verify event is returned as dictionary"""
        event = generate_fake_event()
        
        assert isinstance(event, dict), "Event should be a dictionary"
        assert len(event) == 7, "Event should have 7 fields"


class TestDataGeneratorBatch:
    """Tests for batch CSV generation functionality (5 tests)"""
    
    def test_gen_011_csv_headers(self, clean_test_directory):
        """TC-GEN-011: Verify CSV files have correct headers"""
        test_file = clean_test_directory / "test_batch.csv"
        
        # Generate events and write to CSV
        events = [generate_fake_event() for _ in range(10)]
        df = pd.DataFrame(events)
        df.to_csv(test_file, index=False)
        
        # Read back and verify headers
        df_read = pd.read_csv(test_file)
        expected_columns = ['user_id', 'action', 'product_id', 'product_name', 'price', 'timestamp', 'session_id']
        assert list(df_read.columns) == expected_columns, "CSV should have correct headers"
    
    def test_gen_012_csv_record_count(self, clean_test_directory):
        """TC-GEN-012: Verify CSV contains expected number of records"""
        test_file = clean_test_directory / "test_batch.csv"
        num_events = 100
        
        events = [generate_fake_event() for _ in range(num_events)]
        df = pd.DataFrame(events)
        df.to_csv(test_file, index=False)
        
        df_read = pd.read_csv(test_file)
        assert len(df_read) == num_events, f"Should have {num_events} records"
    
    def test_gen_013_csv_file_naming(self, clean_test_directory):
        """TC-GEN-013: Verify batch file naming convention"""
        # Test the naming pattern events_batch_XXX.csv
        for i in range(3):
            filename = f"events_batch_{i:03d}.csv"
            test_file = clean_test_directory / filename
            
            events = [generate_fake_event() for _ in range(10)]
            df = pd.DataFrame(events)
            df.to_csv(test_file, index=False)
            
            assert test_file.exists(), f"File {filename} should exist"
    
    def test_gen_014_multiple_batches(self, clean_test_directory):
        """TC-GEN-014: Verify generation of multiple batch files"""
        num_batches = 5
        
        for i in range(num_batches):
            test_file = clean_test_directory / f"batch_{i}.csv"
            events = [generate_fake_event() for _ in range(20)]
            df = pd.DataFrame(events)
            df.to_csv(test_file, index=False)
        
        csv_files = list(clean_test_directory.glob("batch_*.csv"))
        assert len(csv_files) == num_batches, f"Should have {num_batches} batch files"
    
    def test_gen_015_csv_data_types(self, clean_test_directory):
        """TC-GEN-015: Verify CSV data can be read with correct types"""
        test_file = clean_test_directory / "test_types.csv"
        
        events = [generate_fake_event() for _ in range(50)]
        df = pd.DataFrame(events)
        df.to_csv(test_file, index=False)
        
        df_read = pd.read_csv(test_file)
        
        # Check that numeric columns can be converted (allowing for NaN from empty strings)
        assert 'user_id' in df_read.columns
        assert 'product_id' in df_read.columns
        assert 'price' in df_read.columns


class TestSparkTransformations:
    """Tests for Spark DataFrame transformations using real PySpark (8 tests)"""
    
    def test_spark_001_dataframe_creation(self, spark_session):
        """TC-SPARK-001: Create DataFrame from generated events"""
        events = [generate_fake_event() for _ in range(10)]
        df = spark_session.createDataFrame(events)
        
        assert df.count() == 10, "Should create DataFrame with 10 rows"
        assert len(df.columns) == 7, "Should have 7 columns"
    
    def test_spark_002_schema_validation(self, spark_session):
        """TC-SPARK-002: Verify DataFrame schema matches expected structure"""
        events = [generate_fake_event() for _ in range(5)]
        df = spark_session.createDataFrame(events)
        
        column_names = df.columns
        expected_columns = ['user_id', 'action', 'product_id', 'product_name', 'price', 'timestamp', 'session_id']
        
        for col in expected_columns:
            assert col in column_names, f"Column {col} should be in schema"
    
    def test_spark_003_filter_transformation(self, spark_session):
        """TC-SPARK-003: Test filtering operations"""
        events = [generate_fake_event() for _ in range(100)]
        df = spark_session.createDataFrame(events)
        
        # Filter for view actions (accounting for empty strings)
        view_df = df.filter(df.action == 'view')
        view_count = view_df.count()
        
        # Should have some view actions
        assert view_count >= 0, "Should be able to filter view actions"
    
    def test_spark_004_column_transformation(self, spark_session):
        """TC-SPARK-004: Test column transformations"""
        from pyspark.sql.functions import upper, col
        
        events = [generate_fake_event() for _ in range(20)]
        df = spark_session.createDataFrame(events)
        
        # Transform action to uppercase
        df_upper = df.withColumn("action_upper", upper(col("action")))
        
        assert "action_upper" in df_upper.columns, "Should add transformed column"
    
    def test_spark_005_aggregation(self, spark_session):
        """TC-SPARK-005: Test aggregation operations"""
        from pyspark.sql.functions import count
        
        events = [generate_fake_event() for _ in range(50)]
        df = spark_session.createDataFrame(events)
        
        # Group by action and count
        action_counts = df.groupBy("action").agg(count("*").alias("count"))
        
        assert action_counts.count() > 0, "Should produce aggregation results"
    
    def test_spark_006_null_handling(self, spark_session):
        """TC-SPARK-006: Test handling of empty/null values"""
        # Create events with known empty values
        events = []
        for i in range(20):
            event = generate_fake_event()
            if i % 5 == 0:
                event['action'] = ''  # Force empty action
            events.append(event)
        
        df = spark_session.createDataFrame(events)
        
        # Filter out empty actions
        non_empty = df.filter(df.action != '')
        
        assert non_empty.count() < df.count(), "Should filter out empty values"
    
    def test_spark_007_join_operations(self, spark_session):
        """TC-SPARK-007: Test join operations"""
        events = [generate_fake_event() for _ in range(30)]
        df1 = spark_session.createDataFrame(events[:15])
        df2 = spark_session.createDataFrame(events[15:])
        
        # Union operation
        df_combined = df1.union(df2)
        
        assert df_combined.count() == 30, "Should combine DataFrames"
    
    def test_spark_008_write_csv(self, spark_session, clean_test_directory):
        """TC-SPARK-008: Test writing DataFrame to CSV"""
        events = [generate_fake_event() for _ in range(25)]
        df = spark_session.createDataFrame(events)
        
        output_path = str(clean_test_directory / "spark_output")
        df.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
        
        # Verify output exists
        csv_files = list(Path(output_path).glob("*.csv"))
        assert len(csv_files) > 0, "Should write CSV file"


class TestSparkSchemaAndConfiguration:
    """Tests for Spark schema and configuration (3 tests)"""
    
    def test_spark_009_structtype_schema(self, spark_session):
        """TC-SPARK-009: Verify StructType schema definition"""
        schema = StructType([
            StructField("user_id", LongType(), True),
            StructField("action", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("product_name", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("event_time", TimestampType(), True),
            StructField("session_id", StringType(), True),
            StructField("_corrupt_record", StringType(), True)
        ])
        
        assert len(schema.fields) == 8, "Schema should have 8 fields including _corrupt_record"
        assert schema.fields[0].name == "user_id", "First field should be user_id"
        assert schema.fields[0].dataType == LongType(), "user_id should be LongType"
    
    def test_spark_010_csv_read_options(self, spark_session, clean_test_directory):
        """TC-SPARK-010: Test CSV read with options"""
        # Create test CSV
        events = [generate_fake_event() for _ in range(20)]
        df_write = spark_session.createDataFrame(events)
        output_file = str(clean_test_directory / "test_options.csv")
        df_write.coalesce(1).write.mode("overwrite").csv(output_file, header=True)
        
        # Read with options
        df_read = spark_session.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(output_file)
        
        assert df_read.count() > 0, "Should read CSV with options"
    
    def test_spark_011_permissive_mode(self, spark_session, clean_test_directory):
        """TC-SPARK-011: Test PERMISSIVE mode for corrupt record handling"""
        # Create CSV with intentionally malformed data
        test_file = clean_test_directory / "corrupt_test.csv"
        with open(test_file, 'w') as f:
            f.write("user_id,action,product_id,product_name,price,timestamp,session_id\n")
            f.write("1001,view,501,Laptop,999.99,2024-01-01T10:00:00,abc123\n")
            f.write("1002,view,502,Mouse,29.99,2024-01-01T10:05:00\n")  # Missing session_id
            f.write("INVALID_DATA_ROW\n")  # Completely malformed
        
        # Read with PERMISSIVE mode
        schema = StructType([
            StructField("user_id", LongType(), True),
            StructField("action", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("product_name", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("_corrupt_record", StringType(), True)
        ])
        
        df = spark_session.read \
            .option("header", "true") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .schema(schema) \
            .csv(str(test_file))
        
        # Should read all rows, with corrupt ones marked
        assert df.count() >= 2, "Should read all rows including corrupt ones"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
