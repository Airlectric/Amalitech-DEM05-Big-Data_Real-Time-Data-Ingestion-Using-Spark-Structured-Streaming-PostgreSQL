"""
Shared pytest fixtures for test_pipeline.py and test_integration.py
"""

import os
import sys
import pytest
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

load_dotenv()


# Test Configuration
BASE_DIR = Path(__file__).parent.parent
TEST_DATA_DIR = BASE_DIR / "data" / "test_events"
DATA_DIR = BASE_DIR / "data" / "events"

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "ecommerce_database"),
    "user": os.getenv("DB_USER", "data_user"),
    "password": os.getenv("DB_PASS", "strongpassword")
}


@pytest.fixture(scope="module")
def spark_session():
    """Create Spark session for tests"""
    spark = SparkSession.builder \
        .appName("PipelineTests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


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


@pytest.fixture(scope="module")
def db_connection():
    """Create database connection for integration tests"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        yield conn
        conn.close()
    except psycopg2.OperationalError as e:
        pytest.skip(f"Cannot connect to database: {e}")


@pytest.fixture(scope="module")
def event_data_dir():
    """Return the events data directory path"""
    return DATA_DIR


@pytest.fixture(scope="session")
def db_config():
    """Return database configuration"""
    return DB_CONFIG
