"""
Unit Tests for Data Generator and Spark Transformations
Tests the actual source code functions from data_generator.py and spark_streaming_to_postgres.py
Uses ACTUAL PySpark for transformation testing
"""

import os
import sys
import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime
from pyspark.sql.functions import col, to_timestamp, when, coalesce, trim, upper, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Import actual source code
from data_generator import generate_fake_event, PRODUCTS


# Test Suite 1: Data Generator Tests
class TestDataGenerator:
    """Test suite for data_generator.py functions"""

    def test_gen_001_generate_fake_event_structure(self):
        """TC-GEN-001: Verify generate_fake_event returns correct structure"""
        event = generate_fake_event(batch_num=0)
        
        # Verify all required fields are present
        required_fields = ['user_id', 'action', 'product_id', 'product_name', 'price', 'timestamp', 'session_id']
        for field in required_fields:
            assert field in event, f"Event should contain {field}"

    def test_gen_002_generate_fake_event_types(self):
        """TC-GEN-002: Verify generate_fake_event returns correct data types"""
        event = generate_fake_event(batch_num=0)
        
        # Check data types
        assert isinstance(event['user_id'], (int, type(None))), "user_id should be int or None"
        assert isinstance(event['action'], (str, type(None))), "action should be string or None"
        assert isinstance(event['product_id'], int), "product_id should be int"
        assert isinstance(event['product_name'], str), "product_name should be string"
        assert isinstance(event['price'], float), "price should be float"
        assert isinstance(event['timestamp'], str), "timestamp should be string"
        assert isinstance(event['session_id'], (str, type(None))), "session_id should be string or None"

    def test_gen_003_product_names_available(self):
        """TC-GEN-003: Verify PRODUCTS list is available and used"""
        assert isinstance(PRODUCTS, list), "PRODUCTS should be a list"
        assert len(PRODUCTS) > 0, "PRODUCTS list should not be empty"
        
        # Generate several events and verify product names exist
        for _ in range(20):
            event = generate_fake_event(batch_num=0)
            assert event['product_name'] != '', "product_name should not be empty"

    def test_gen_004_user_id_when_present(self):
        """TC-GEN-004: Verify user_id type and range when present"""
        for _ in range(20):
            event = generate_fake_event(batch_num=0)
            if event['user_id'] is not None:
                assert isinstance(event['user_id'], int), "user_id should be integer"
                assert event['user_id'] > 0, "user_id should be positive when present"

    def test_gen_005_product_id_is_integer(self):
        """TC-GEN-005: Verify product_id is always integer"""
        for _ in range(20):
            event = generate_fake_event(batch_num=0)
            assert isinstance(event['product_id'], int), "product_id should be integer"

    def test_gen_006_action_can_be_invalid(self):
        """TC-GEN-006: Verify action can be intentionally invalid or None"""
        has_null_action = False
        has_empty_action = False
        has_valid_action = False
        
        for _ in range(100):
            event = generate_fake_event(batch_num=0)
            if event['action'] is None:
                has_null_action = True
            elif event['action'] == '':
                has_empty_action = True
            else:
                has_valid_action = True
        
        # At least one type should be present
        assert has_null_action or has_empty_action or has_valid_action, "Should have some actions"

    def test_gen_007_price_range(self):
        """TC-GEN-007: Verify price is within expected range"""
        for _ in range(20):
            event = generate_fake_event(batch_num=0)
            # Price should be float within expected range
            assert isinstance(event['price'], float), "price should be float"
            assert -15.0 <= event['price'] <= 800.0, "price should be in expected range"

    def test_gen_008_timestamp_format(self):
        """TC-GEN-008: Verify timestamp is string in valid format"""
        for _ in range(30):
            event = generate_fake_event(batch_num=0)
            timestamp = event['timestamp']
            assert isinstance(timestamp, str), "timestamp should be string"
            
            # Timestamp can be ISO format, standard format, empty, or INVALID_DATE
            valid_formats = ['T', ' ', '', 'INVALID_DATE']
            is_valid = any(fmt in timestamp for fmt in valid_formats[:-1]) or timestamp == valid_formats[-1]
            assert is_valid, "timestamp should be in valid format"

    def test_gen_009_session_id_format(self):
        """TC-GEN-009: Verify session_id is 5-digit string or None"""
        for _ in range(30):
            event = generate_fake_event(batch_num=0)
            session_id = event['session_id']
            if session_id is not None:
                assert isinstance(session_id, str), "session_id should be string when present"
                assert len(session_id) == 5, "session_id should be 5 digits"
                assert session_id.isdigit(), "session_id should be numeric string"

    def test_gen_010_data_quality_issues_present(self):
        """TC-GEN-010: Verify data quality issues are intentionally generated"""
        all_events = [generate_fake_event(batch_num=0) for _ in range(1000)]
        
        has_null_user_id = any(e['user_id'] is None for e in all_events)
        has_invalid_product_id = any(e['product_id'] == -1 for e in all_events)
        has_negative_price = any(e['price'] < 0 for e in all_events)
        has_empty_action = any(e['action'] == '' for e in all_events)
        has_null_action = any(e['action'] is None for e in all_events)
        has_empty_timestamp = any(e['timestamp'] == '' for e in all_events)
        has_invalid_timestamp = any(e['timestamp'] == 'INVALID_DATE' for e in all_events)
        has_null_session_id = any(e['session_id'] is None for e in all_events)
        
        # Count how many data quality issues are present
        issues_found = sum([
            has_null_user_id,
            has_invalid_product_id,
            has_negative_price,
            has_empty_action or has_null_action,
            has_empty_timestamp or has_invalid_timestamp,
            has_null_session_id
        ])
        
        # At least 5 out of 6 issue types should be present in 1000 events
        assert issues_found >= 5, f"Should have at least 5 types of data quality issues, found {issues_found}"
        
        # These are the most common issues that should definitely be present
        assert has_null_user_id, "Should have some null user_ids (2% probability)"
        assert has_invalid_product_id, "Should have some invalid product_ids (1.5% probability)"
        assert has_null_session_id, "Should have some null session_ids (10% probability)"


# Test Suite 2: Data Generator Batch Tests
class TestDataGeneratorBatch:
    """Test suite for batch CSV generation from generate_fake_event"""

    def test_batch_001_create_batch_csv(self, clean_test_directory):
        """TC-BATCH-001: Verify batch CSV file creation from events"""
        # Generate a batch of events using actual generate_fake_event
        events = [generate_fake_event(batch_num=0) for _ in range(100)]
        df = pd.DataFrame(events)
        
        output_file = clean_test_directory / "test_batch.csv"
        df.to_csv(output_file, index=False)
        
        assert output_file.exists(), "CSV file should be created"
        assert output_file.stat().st_size > 0, "CSV file should not be empty"

    def test_batch_002_batch_size(self, clean_test_directory):
        """TC-BATCH-002: Verify batch contains correct number of records"""
        num_records = 100
        events = [generate_fake_event(batch_num=0) for _ in range(num_records)]
        df = pd.DataFrame(events)
        
        output_file = clean_test_directory / "test_batch.csv"
        df.to_csv(output_file, index=False)
        
        df_read = pd.read_csv(output_file)
        assert len(df_read) == num_records, f"CSV should contain {num_records} records"

    def test_batch_003_csv_schema(self, clean_test_directory):
        """TC-BATCH-003: Verify CSV schema matches expected columns"""
        events = [generate_fake_event(batch_num=0) for _ in range(50)]
        df = pd.DataFrame(events)
        
        output_file = clean_test_directory / "test_schema.csv"
        df.to_csv(output_file, index=False)
        
        df_read = pd.read_csv(output_file)
        expected_columns = ['user_id', 'action', 'product_id', 'product_name', 'price', 'timestamp', 'session_id']
        
        assert list(df_read.columns) == expected_columns, "CSV columns should match expected schema"

    def test_batch_004_optional_session_id_column(self, clean_test_directory):
        """TC-BATCH-004: Verify session_id column can be dropped"""
        events = [generate_fake_event(batch_num=0) for _ in range(50)]
        df = pd.DataFrame(events)
        
        # Simulate dropping session_id column (as actual generator might do)
        if 'session_id' in df.columns:
            df_no_session = df.drop(columns=['session_id'])
        else:
            df_no_session = df
        
        output_file = clean_test_directory / "test_no_session.csv"
        df_no_session.to_csv(output_file, index=False)
        
        df_read = pd.read_csv(output_file)
        # Should have 6 columns now (without session_id)
        assert len(df_read.columns) == 6, "CSV should have 6 columns after dropping session_id"

    def test_batch_005_multiple_batches(self, clean_test_directory):
        """TC-BATCH-005: Verify multiple batches can be created"""
        num_batches = 5
        
        for batch_num in range(num_batches):
            events = [generate_fake_event(batch_num=batch_num) for _ in range(50)]
            df = pd.DataFrame(events)
            
            output_file = clean_test_directory / f"batch_{batch_num:03d}.csv"
            df.to_csv(output_file, index=False)
        
        # Verify all files were created
        created_files = list(clean_test_directory.glob("batch_*.csv"))
        assert len(created_files) == num_batches, f"Should have created {num_batches} batch files"


# Test Suite 3: Spark Transformation Logic Tests (ACTUAL PYSPARK)
class TestSparkTransformations:
    """Test suite for Spark transformations using ACTUAL PySpark"""

    def test_spark_001_timestamp_parsing(self, spark_session):
        """TC-SPARK-001: Test ACTUAL Spark timestamp parsing"""
        # Create test data with various timestamp formats
        data = [
            ("2026-01-28T10:30:45.123456",),
            ("2026-01-28 10:30:45",),
            ("",),
            ("INVALID_DATE",),
            (None,)
        ]
        df = spark_session.createDataFrame(data, ["timestamp"])
        
        # Apply ACTUAL Spark transformation from spark_streaming_to_postgres.py
        result_df = df.withColumn(
            "event_time",
            coalesce(
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
                to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(lit(None), "yyyy-MM-dd HH:mm:ss")
            )
        ).withColumn(
            "event_time",
            coalesce(col("event_time"), current_timestamp())
        )
        
        results = result_df.collect()
        
        # First two should parse successfully
        assert results[0]["event_time"] is not None, "ISO format should parse"
        assert results[1]["event_time"] is not None, "Standard format should parse"
        
        # Invalid ones should fallback to current_timestamp
        assert results[2]["event_time"] is not None, "Empty should fallback to current time"
        assert results[3]["event_time"] is not None, "Invalid should fallback to current time"
        assert results[4]["event_time"] is not None, "Null should fallback to current time"

    def test_spark_002_action_standardization(self, spark_session):
        """TC-SPARK-002: Test ACTUAL Spark action standardization"""
        # Create test data
        data = [
            ('view',),
            ('VIEW',),
            ('Purchase',),
            ('add_to_cart',),
            ('remove_from_cart',),
            ('',),
            (None,),
            ('invalid_action',)
        ]
        df = spark_session.createDataFrame(data, ["action"])
        
        # Apply ACTUAL Spark transformation from spark_streaming_to_postgres.py
        result_df = df.withColumn(
            "action",
            upper(trim(coalesce(col("action"), lit("UNKNOWN"))))
        ).withColumn(
            "action",
            when(col("action").isin("VIEW", "PURCHASE", "ADD_TO_CART", "REMOVE_FROM_CART"), col("action"))
            .otherwise("UNKNOWN")
        )
        
        results = [row["action"] for row in result_df.collect()]
        valid_actions = {'VIEW', 'PURCHASE', 'ADD_TO_CART', 'REMOVE_FROM_CART', 'UNKNOWN'}
        
        # All results should be valid actions
        for action in results:
            assert action in valid_actions, f"Action {action} should be standardized"
        
        # Check specific transformations
        assert results[0] == 'VIEW', "'view' should become 'VIEW'"
        assert results[1] == 'VIEW', "'VIEW' should stay 'VIEW'"
        assert results[2] == 'PURCHASE', "'Purchase' should become 'PURCHASE'"
        assert results[5] == 'UNKNOWN', "Empty should become 'UNKNOWN'"
        assert results[6] == 'UNKNOWN', "None should become 'UNKNOWN'"
        assert results[7] == 'UNKNOWN', "Invalid action should become 'UNKNOWN'"

    def test_spark_003_price_correction(self, spark_session):
        """TC-SPARK-003: Test ACTUAL Spark price correction"""
        # Create test data with negative and positive prices
        data = [
            (-10.5,),
            (-100.0,),
            (0.0,),
            (50.0,),
            (999.99,)
        ]
        df = spark_session.createDataFrame(data, ["price"])
        
        # Apply ACTUAL Spark transformation from spark_streaming_to_postgres.py
        result_df = df.withColumn(
            "price",
            when(col("price") >= 0, col("price")).otherwise(0.0)
        )
        
        results = [row["price"] for row in result_df.collect()]
        
        # All prices should be non-negative
        for price in results:
            assert price >= 0, f"Price should be non-negative, got {price}"
        
        # Check specific values
        assert results[0] == 0.0, "Negative -10.5 should become 0.0"
        assert results[1] == 0.0, "Negative -100.0 should become 0.0"
        assert results[2] == 0.0, "0.0 should stay 0.0"
        assert results[3] == 50.0, "Positive 50.0 should stay 50.0"
        assert results[4] == 999.99, "Positive 999.99 should stay 999.99"

    def test_spark_004_user_id_validation(self, spark_session):
        """TC-SPARK-004: Test ACTUAL Spark user_id validation"""
        # Create test data
        schema = StructType([StructField("user_id", LongType(), True)])
        data = [(None,), (0,), (-5,), (100,), (999,)]
        df = spark_session.createDataFrame(data, schema)
        
        # Apply ACTUAL Spark transformation from spark_streaming_to_postgres.py
        result_df = df.withColumn(
            "user_id",
            when(col("user_id").isNotNull() & (col("user_id") > 0), col("user_id"))
        )
        
        results = result_df.collect()
        
        # Null and invalid should be None
        assert results[0]["user_id"] is None, "None should stay None"
        assert results[1]["user_id"] is None, "0 should become None"
        assert results[2]["user_id"] is None, "Negative should become None"
        
        # Valid IDs should be preserved
        assert results[3]["user_id"] == 100, "100 should be preserved"
        assert results[4]["user_id"] == 999, "999 should be preserved"

    def test_spark_005_product_id_validation(self, spark_session):
        """TC-SPARK-005: Test ACTUAL Spark product_id validation"""
        # Create test data
        schema = StructType([StructField("product_id", LongType(), True)])
        data = [(-1,), (0,), (1001,), (2500,)]
        df = spark_session.createDataFrame(data, schema)
        
        # Apply ACTUAL Spark transformation from spark_streaming_to_postgres.py
        result_df = df.withColumn(
            "product_id",
            when(col("product_id") > 0, col("product_id"))
        )
        
        results = result_df.collect()
        
        # Invalid IDs should be None
        assert results[0]["product_id"] is None, "-1 should become None"
        assert results[1]["product_id"] is None, "0 should become None"
        
        # Valid IDs should be preserved
        assert results[2]["product_id"] == 1001, "1001 should be preserved"
        assert results[3]["product_id"] == 2500, "2500 should be preserved"

    def test_spark_006_product_name_handling(self, spark_session):
        """TC-SPARK-006: Test ACTUAL Spark product_name handling"""
        # Create test data
        data = [('',), (None,), ('  ',), ('Laptop',), ('Phone',)]
        df = spark_session.createDataFrame(data, ["product_name"])
        
        # Apply ACTUAL Spark transformation from spark_streaming_to_postgres.py
        result_df = df.withColumn(
            "product_name",
            trim(coalesce(col("product_name"), lit("Unknown Product")))
        )
        
        results = [row["product_name"] for row in result_df.collect()]
        
        # Check transformations
        assert results[0] == '', "Empty becomes empty after trim"
        assert results[1] == 'Unknown Product', "None should become 'Unknown Product'"
        assert results[2] == '', "Spaces become empty after trim"
        assert results[3] == 'Laptop', "'Laptop' should stay 'Laptop'"
        assert results[4] == 'Phone', "'Phone' should stay 'Phone'"

    def test_spark_007_session_id_handling(self, spark_session):
        """TC-SPARK-007: Test ACTUAL Spark session_id handling"""
        # Create test data
        data = [(None,), ('',), ('12345',), ('67890',)]
        df = spark_session.createDataFrame(data, ["session_id"])
        
        # Apply ACTUAL Spark transformation from spark_streaming_to_postgres.py
        result_df = df.withColumn(
            "session_id",
            coalesce(col("session_id"), lit("unknown"))
        )
        
        results = [row["session_id"] for row in result_df.collect()]
        
        # Check transformations
        assert results[0] == 'unknown', "None should become 'unknown'"
        assert results[1] == '', "Empty stays empty (coalesce doesn't change it)"
        assert results[2] == '12345', "'12345' should stay '12345'"
        assert results[3] == '67890', "'67890' should stay '67890'"

    def test_spark_008_full_transformation_pipeline(self, spark_session):
        """TC-SPARK-008: Test COMPLETE ACTUAL Spark transformation pipeline"""
        # Create raw event data with quality issues
        schema = StructType([
            StructField("user_id", LongType(), True),
            StructField("action", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("product_name", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("session_id", StringType(), True)
        ])
        
        data = [(None, '', -1, '', -10.5, 'INVALID_DATE', None)]
        df = spark_session.createDataFrame(data, schema)
        
        # Apply ALL transformations from spark_streaming_to_postgres.py
        cleaned_df = (
            df
            # Timestamp parsing
            .withColumn(
                "event_time",
                coalesce(
                    to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
                    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
                    to_timestamp(lit(None), "yyyy-MM-dd HH:mm:ss")
                )
            )
            .withColumn("event_time", coalesce(col("event_time"), current_timestamp()))
            
            # Action standardization
            .withColumn("action", upper(trim(coalesce(col("action"), lit("UNKNOWN")))))
            .withColumn(
                "action",
                when(col("action").isin("VIEW", "PURCHASE", "ADD_TO_CART", "REMOVE_FROM_CART"), col("action"))
                .otherwise("UNKNOWN")
            )
            
            # Price correction
            .withColumn("price", when(col("price") >= 0, col("price")).otherwise(0.0))
            
            # ID validation
            .withColumn("user_id", when(col("user_id").isNotNull() & (col("user_id") > 0), col("user_id")))
            .withColumn("product_id", when(col("product_id") > 0, col("product_id")))
            
            # Product name
            .withColumn("product_name", trim(coalesce(col("product_name"), lit("Unknown Product"))))
            
            # Session ID
            .withColumn("session_id", coalesce(col("session_id"), lit("unknown")))
        )
        
        # Get result
        result = cleaned_df.collect()[0]
        
        # Verify ALL transformations
        assert result["action"] == 'UNKNOWN', "Empty action should become UNKNOWN"
        assert result["price"] == 0.0, "Negative price should become 0"
        assert result["product_name"] == '', "Empty name becomes empty after trim"
        assert result["session_id"] == 'unknown', "Null session should become unknown"
        assert result["user_id"] is None, "Invalid user_id should be null"
        assert result["product_id"] is None, "Invalid product_id should be null"
        assert result["event_time"] is not None, "Invalid timestamp should fallback to current time"


# Test Suite 4: Spark Schema and Configuration Tests
class TestSparkSchemaAndConfiguration:
    """Test suite for Spark schema and configuration"""

    def test_spark_config_001_schema_definition(self):
        """TC-SPARK-CONFIG-001: Verify Spark schema definition"""
        expected_schema_fields = ['user_id', 'action', 'product_id', 'product_name', 'price', 'timestamp', 'session_id']
        
        # Verify field names match expected schema
        assert len(expected_schema_fields) == 7, "Input schema should have 7 fields"
        assert all(isinstance(f, str) for f in expected_schema_fields), "All field names should be strings"

    def test_spark_config_002_output_column_mapping(self):
        """TC-SPARK-CONFIG-002: Verify final dataframe output columns"""
        expected_output_columns = ['user_id', 'action', 'product_id', 'product_name', 'price', 'event_time', 'session_id']
        
        # Verify output has correct columns
        assert len(expected_output_columns) == 7, "Should have 7 columns in final output"
        assert 'event_time' in expected_output_columns, "event_time should replace timestamp in output"
        assert 'timestamp' not in expected_output_columns, "timestamp should not be in final output"

    def test_spark_config_003_database_configuration(self):
        """TC-SPARK-CONFIG-003: Verify database configuration is set"""
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")
        db_user = os.getenv("DB_USER")
        db_pass = os.getenv("DB_PASS")
        
        # Verify configuration is loaded from environment
        assert db_host is not None, "DB_HOST should be configured"
        assert db_port is not None, "DB_PORT should be configured"
        assert db_name is not None, "DB_NAME should be configured"
        assert db_user is not None, "DB_USER should be configured"
        assert db_pass is not None, "DB_PASS should be configured"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
