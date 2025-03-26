from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnan, sum as spark_sum, when


def validate_data_quality(spark, stock_dataframe, ds):
    """
    Performs data quality checks on stock data including:
    1. Deduplication verification - identifies duplicate records
    2. Null value detection - identifies records with null values
    3. Basic aggregation - provides summary statistics
    
    Args:
        spark: SparkSession object
        stock_dataframe: DataFrame containing stock data
        ds: Date string for the partition
        
    Returns:
        DataFrame with data quality metrics
    """
    # Register the DataFrame as a temporary view
    stock_dataframe.createOrReplaceTempView("stocks")
    
    # Check for duplicate records based on symbol and timestamp
    duplicate_check = spark.sql(f"""
        SELECT 
            symbol,
            date,
            COUNT(*) as record_count,
            '{ds}' as ds
        FROM stocks
        WHERE ds = '{ds}'
        GROUP BY symbol, date
        HAVING COUNT(*) > 1
    """)
    
    # Check for null values in important fields
    null_check = stock_dataframe.filter(
        (col("ds") == ds) &
        (
            col("symbol").isNull() |
            col("open").isNull() |
            col("high").isNull() |
            col("low").isNull() |
            col("close").isNull() |
            col("volume").isNull()
        )
    ).select(
        col("symbol"),
        col("date"),
        when(col("symbol").isNull(), 1).otherwise(0).alias("null_symbol"),
        when(col("open").isNull(), 1).otherwise(0).alias("null_open"),
        when(col("high").isNull(), 1).otherwise(0).alias("null_high"),
        when(col("low").isNull(), 1).otherwise(0).alias("null_low"),
        when(col("close").isNull(), 1).otherwise(0).alias("null_close"),
        when(col("volume").isNull(), 1).otherwise(0).alias("null_volume"),
        col("ds")
    )
    
    # Basic aggregation for summary stats
    summary_stats = spark.sql(f"""
        SELECT
            symbol,
            COUNT(*) as record_count,
            AVG(close) as avg_close_price,
            MAX(high) as max_high_price,
            MIN(low) as min_low_price,
            SUM(volume) as total_volume,
            '{ds}' as ds
        FROM stocks
        WHERE ds = '{ds}'
        GROUP BY symbol, ds
    """)
    
    # Combine results into a consolidated data quality report
    duplicate_check.createOrReplaceTempView("duplicates")
    null_check.createOrReplaceTempView("nulls")
    summary_stats.createOrReplaceTempView("stats")
    
    # Final quality report
    quality_report = spark.sql(f"""
        SELECT
            s.symbol,
            s.record_count as total_records,
            COALESCE(d.record_count, 0) as duplicate_records,
            COALESCE(SUM(n.null_open + n.null_high + n.null_low + n.null_close + n.null_volume), 0) as total_null_values,
            s.avg_close_price,
            s.max_high_price,
            s.min_low_price,
            s.total_volume,
            '{ds}' as ds
        FROM stats s
        LEFT JOIN duplicates d ON s.symbol = d.symbol
        LEFT JOIN nulls n ON s.symbol = n.symbol
        GROUP BY s.symbol, s.record_count, d.record_count, s.avg_close_price, s.max_high_price, s.min_low_price, s.total_volume, ds
    """)
    
    return quality_report


def main():
    ds = '2023-08-01'
    spark = SparkSession.builder \
        .appName("stock_data_quality") \
        .getOrCreate()
        
    # Get stock data
    stock_data = spark.table("bootcamp.stock_prices")
    
    # Run data quality validation
    quality_report = validate_data_quality(spark, stock_data, ds)
    
    # Write the quality report to a table
    quality_report.write.mode("overwrite").insertInto("stock_data_quality_metrics")
    
    # Optional: Display results for immediate review
    print("Data quality validation complete. Results written to stock_data_quality_metrics table.")
    print(f"Summary for date: {ds}")
    quality_report.show(10)


if __name__ == "__main__":
    main()