# Stock Market Analysis Pipeline
A comprehensive data engineering solution for fetching, processing, and analyzing stock market data from the Polygon API using Apache Spark.

## Overview
This project implements a scalable ETL pipeline that harvests stock ticker data, processes it efficiently, and stores it using an optimized array-based monthly aggregation pattern for compact storage and fast analysis.

## Key Features
- Parallelized API Data Collection: Uses Spark's distributed computing to fetch stock data with 4x faster ingestion speeds
- Efficient Storage Schema: Implements list-based temporal storage for stock metrics enabling efficient storage and analysis
- Robust Data Quality: Built-in verification for deduplication, null value detection, and noise filtering
- Comprehensive Testing: Unit tests validating core transformation logic using pytest and chispa
- Optimized Performance: Handles ~11,000 API calls per minute while maintaining cost efficiency
## Architecture
1. Data Extraction: Collects stock ticker data from Polygon API using parallel executors
2. Data Transformation: Processes raw API responses into structured datasets
3. Data Storage: Stores data using Apache Iceberg with optimized partitioning
4. Data Quality: Validates data completeness and accuracy through automated checks
5. Data Analysis: Enables easy historical analysis through SQL queries
## Technologies
- Apache Spark for distributed processing
- AWS Glue for job execution
- Apache Iceberg for table format
- Polygon API for financial data
- Python for transformation logic
- pytest for testing
