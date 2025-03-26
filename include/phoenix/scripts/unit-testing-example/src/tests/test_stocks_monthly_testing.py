from pyspark.testing import assertDataFrameEqual
from ..jobs.data_quality_job import validate_data_quality
from collections import namedtuple
import pytest

# Define named tuples for test data
StockData = namedtuple("StockData", "symbol date open high low close volume ds")
DataQualityReport = namedtuple("DataQualityReport", "symbol total_records duplicate_records total_null_values avg_close_price max_high_price min_low_price total_volume ds")

def test_data_quality_checks(spark):
    """Test data quality validation with sample data including duplicates and nulls"""
    ds = "2023-08-01"
    
    # Test data with various quality issues
    stock_data = [
        # Regular data
        StockData(symbol="AAPL", date="2023-08-01", open=180.0, high=185.0, low=179.5, close=182.5, volume=1000000, ds=ds),
        # Duplicate record (same symbol and date)
        StockData(symbol="AAPL", date="2023-08-01", open=180.5, high=186.0, low=180.0, close=183.0, volume=1050000, ds=ds),
        # Record with null values
        StockData(symbol="MSFT", date="2023-08-01", open=None, high=320.0, low=315.0, close=None, volume=800000, ds=ds),
        # Normal record for MSFT
        StockData(symbol="MSFT", date="2023-08-01", open=310.0, high=325.0, low=310.0, close=323.0, volume=750000, ds=ds),
        # Record with different date partition - should be filtered out
        StockData(symbol="GOOG", date="2023-08-01", open=120.0, high=125.0, low=118.0, close=123.0, volume=500000, ds="2023-07-01")
    ]
    
    stock_df = spark.createDataFrame(stock_data)
    actual_df = validate_data_quality(spark, stock_df, ds)
    
    # Expected results after quality checks
    expected_values = [
        DataQualityReport(
            symbol="AAPL",
            total_records=2,          # Total AAPL records
            duplicate_records=2,      # Both are duplicates (same symbol+date)
            total_null_values=0,      # No nulls
            avg_close_price=182.75,   # Average of 182.5 and 183.0
            max_high_price=186.0,     # Max high value
            min_low_price=179.5,      # Min low value
            total_volume=2050000,     # Sum of volume
            ds=ds
        ),
        DataQualityReport(
            symbol="MSFT",
            total_records=2,          # Total MSFT records
            duplicate_records=2,      # Both are duplicates (same symbol+date)
            total_null_values=2,      # Null open and close in one record
            avg_close_price=323.0,    # Only one non-null close value
            max_high_price=325.0,     # Max high value
            min_low_price=310.0,      # Min low value
            total_volume=1550000,     # Sum of volume
            ds=ds
        )
    ]
    
    expected_df = spark.createDataFrame(expected_values)
    assertDataFrameEqual(actual_df, expected_df)

def test_data_quality_no_issues(spark):
    """Test data quality validation with clean data (no duplicates or nulls)"""
    ds = "2023-08-01"
    
    # Clean test data
    stock_data = [
        StockData(symbol="AAPL", date="2023-08-01", open=180.0, high=185.0, low=179.5, close=182.5, volume=1000000, ds=ds),
        StockData(symbol="MSFT", date="2023-08-01", open=310.0, high=320.0, low=315.0, close=318.0, volume=800000, ds=ds),
        StockData(symbol="GOOGL", date="2023-08-01", open=120.0, high=125.0, low=118.0, close=123.0, volume=500000, ds=ds)
    ]
    
    stock_df = spark.createDataFrame(stock_data)
    actual_df = validate_data_quality(spark, stock_df, ds)
    
    # Expected results - clean data
    expected_values = [
        DataQualityReport(
            symbol="AAPL",
            total_records=1,
            duplicate_records=0,      # No duplicates
            total_null_values=0,      # No nulls
            avg_close_price=182.5,
            max_high_price=185.0,
            min_low_price=179.5,
            total_volume=1000000,
            ds=ds
        ),
        DataQualityReport(
            symbol="MSFT",
            total_records=1,
            duplicate_records=0,      # No duplicates
            total_null_values=0,      # No nulls
            avg_close_price=318.0,
            max_high_price=320.0,
            min_low_price=315.0,
            total_volume=800000,
            ds=ds
        ),
        DataQualityReport(
            symbol="GOOGL",
            total_records=1,
            duplicate_records=0,      # No duplicates
            total_null_values=0,      # No nulls
            avg_close_price=123.0,
            max_high_price=125.0,
            min_low_price=118.0,
            total_volume=500000,
            ds=ds
        )
    ]
    
    expected_df = spark.createDataFrame(expected_values)
    assertDataFrameEqual(actual_df, expected_df)