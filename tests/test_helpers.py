"""
Helper functions for testing the pipeline
"""

import os
import pandas as pd
from datetime import datetime
from pathlib import Path


def create_test_csv(output_dir, filename, num_records=100, include_issues=False):
    """
    Create a test CSV file with sample data
    
    Args:
        output_dir: Directory to save the CSV
        filename: Name of the CSV file
        num_records: Number of records to generate
        include_issues: Whether to include data quality issues
    
    Returns:
        Path to created CSV file
    """
    data = []
    
    for i in range(num_records):
        record = {
            'user_id': i + 1,
            'action': 'view',
            'product_id': 1001 + i,
            'product_name': 'Laptop',
            'price': 999.99,
            'timestamp': datetime.now().isoformat(),
            'session_id': str(10000 + i)
        }
        
        if include_issues:
            # Add data quality issues randomly
            if i % 10 == 0:
                record['user_id'] = None
            if i % 15 == 0:
                record['product_id'] = -1
            if i % 8 == 0:
                record['price'] = -50.0
            if i % 12 == 0:
                record['action'] = ''
            if i % 7 == 0:
                record['timestamp'] = 'INVALID_DATE'
            if i % 9 == 0:
                record['session_id'] = None
        
        data.append(record)
    
    df = pd.DataFrame(data)
    output_path = Path(output_dir) / filename
    df.to_csv(output_path, index=False)
    
    return output_path


def validate_csv_schema(csv_path):
    """
    Validate that CSV has correct schema
    
    Args:
        csv_path: Path to CSV file
    
    Returns:
        Tuple (is_valid, error_message)
    """
    expected_columns = ['user_id', 'action', 'product_id', 'product_name', 
                       'price', 'timestamp', 'session_id']
    
    try:
        df = pd.read_csv(csv_path)
        actual_columns = list(df.columns)
        
        if actual_columns != expected_columns:
            return False, f"Column mismatch. Expected {expected_columns}, got {actual_columns}"
        
        return True, "Schema is valid"
    
    except Exception as e:
        return False, f"Error reading CSV: {str(e)}"


def count_data_issues(csv_path):
    """
    Count data quality issues in a CSV file
    
    Args:
        csv_path: Path to CSV file
    
    Returns:
        Dictionary with counts of each issue type
    """
    df = pd.read_csv(csv_path)
    
    issues = {
        'null_user_id': df['user_id'].isnull().sum(),
        'invalid_product_id': (df['product_id'] < 0).sum(),
        'negative_price': (df['price'] < 0).sum(),
        'empty_action': (df['action'] == '').sum() if 'action' in df.columns else 0,
        'null_action': df['action'].isnull().sum() if 'action' in df.columns else 0,
        'empty_timestamp': (df['timestamp'] == '').sum() if 'timestamp' in df.columns else 0,
        'null_session_id': df['session_id'].isnull().sum() if 'session_id' in df.columns else 0
    }
    
    return issues


def simulate_transformation(df):
    """
    Simulate the transformation logic from Spark streaming job
    
    Args:
        df: Input pandas DataFrame
    
    Returns:
        Transformed DataFrame
    """
    df_copy = df.copy()
    
    # Action standardization
    df_copy['action'] = df_copy['action'].fillna('UNKNOWN')
    df_copy['action'] = df_copy['action'].str.upper().str.strip()
    valid_actions = ['VIEW', 'PURCHASE', 'ADD_TO_CART', 'REMOVE_FROM_CART']
    df_copy['action'] = df_copy['action'].apply(
        lambda x: x if x in valid_actions else 'UNKNOWN'
    )
    
    # Price correction
    df_copy['price'] = df_copy['price'].apply(lambda x: max(0.0, x))
    
    # User ID validation
    df_copy['user_id'] = df_copy['user_id'].apply(
        lambda x: x if pd.notnull(x) and x > 0 else None
    )
    
    # Product ID validation
    df_copy['product_id'] = df_copy['product_id'].apply(
        lambda x: x if x > 0 else None
    )
    
    # Product name handling
    df_copy['product_name'] = df_copy['product_name'].fillna('Unknown Product')
    df_copy['product_name'] = df_copy['product_name'].apply(
        lambda x: x.strip() if x.strip() else 'Unknown Product'
    )
    
    # Session ID handling
    df_copy['session_id'] = df_copy['session_id'].fillna('unknown')
    df_copy['session_id'] = df_copy['session_id'].replace('', 'unknown')
    
    return df_copy


def verify_transformation_results(original_df, transformed_df):
    """
    Verify that transformations were applied correctly
    
    Args:
        original_df: Original DataFrame before transformation
        transformed_df: DataFrame after transformation
    
    Returns:
        Tuple (is_valid, issues_list)
    """
    issues = []
    
    # Check no negative prices
    if (transformed_df['price'] < 0).any():
        issues.append("Found negative prices after transformation")
    
    # Check no invalid actions
    valid_actions = ['VIEW', 'PURCHASE', 'ADD_TO_CART', 'REMOVE_FROM_CART', 'UNKNOWN']
    invalid_actions = transformed_df[~transformed_df['action'].isin(valid_actions)]
    if len(invalid_actions) > 0:
        issues.append(f"Found {len(invalid_actions)} invalid actions")
    
    # Check no null/empty product names
    if transformed_df['product_name'].isnull().any():
        issues.append("Found null product names")
    if (transformed_df['product_name'] == '').any():
        issues.append("Found empty product names")
    
    # Check no null/empty session IDs
    if transformed_df['session_id'].isnull().any():
        issues.append("Found null session IDs")
    if (transformed_df['session_id'] == '').any():
        issues.append("Found empty session IDs")
    
    is_valid = len(issues) == 0
    return is_valid, issues


def generate_performance_metrics(start_time, end_time, num_records):
    """
    Calculate performance metrics
    
    Args:
        start_time: Start timestamp
        end_time: End timestamp
        num_records: Number of records processed
    
    Returns:
        Dictionary with metrics
    """
    duration = end_time - start_time
    throughput = num_records / duration if duration > 0 else 0
    
    metrics = {
        'duration_seconds': round(duration, 3),
        'num_records': num_records,
        'throughput_records_per_second': round(throughput, 2),
        'avg_time_per_record_ms': round((duration / num_records) * 1000, 3) if num_records > 0 else 0
    }
    
    return metrics
