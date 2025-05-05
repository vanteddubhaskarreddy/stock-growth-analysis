# Stock Market Analysis Pipeline

![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Apache_Iceberg-2C2D72?style=for-the-badge&logo=apache&logoColor=white)

A data engineering solution that harvests, processes, and analyzes stock market data at scale using Apache Spark and optimized temporal storage patterns.

## Project Overview

This project implements a high-performance ETL pipeline that collects stock data from Polygon API using distributed processing, transforms it efficiently, and stores it using an innovative array-based monthly aggregation pattern for compact storage and rapid analysis.

Key achievements:
- **4x faster data ingestion** using Spark's parallel processing capabilities
- **Processing ~11,000 API calls per minute** with optimized resource utilization
- **Reduced AWS Glue compute costs** through efficient executor management
- **Simplified historical analysis** with list-based temporal storage schema

## Architecture

![Architecture Diagram](https://fakeimg.pl/800x200/E25A1C/ffffff?text=Stock+Analysis+Architecture)

### Data Flow
1. **Extraction**: Parallel collection of stock tickers and price data from Polygon API
2. **Transformation**: Processing of raw market data into analytical structures
3. **Loading**: Storage in Apache Iceberg tables with optimized partitioning
4. **Quality Assurance**: Automated validation for data completeness and accuracy
5. **Analysis**: Historical and current market trends examination

## Technical Implementation

### Parallel API Data Collection

This project significantly improves data collection performance by:

```python
# Using Spark repartitioning to parallelize API calls
all_data = tickers.repartition(4).withColumn("daily_prices", 
                                           query_api_udf(col("ticker"), lit(run_date)))
```

- Distributing API calls across executors rather than sequential driver execution
- Handling ~11,000 API calls per minute with fault tolerance
- Optimizing resource utilization to reduce AWS Glue compute costs

### List-Based Temporal Storage

A key innovation is the efficient storage of time-series data:

```sql
CREATE TABLE monthly_stock_prices (
  symbol STRING,
  from_date STRING,
  metric_name STRING,
  metric_array ARRAY<DOUBLE>,  -- Stores daily values in an array
  month_start STRING
)
PARTITIONED BY (symbol, month_start, metric_name)
```

- Stores daily metric values (open/close prices, highs/lows, volumes) in arrays by month
- Reduces storage requirements compared to standard row-based storage
- Enables efficient historical analysis through array operations
- Simplifies data management with fewer partitions

### Comprehensive Data Quality Framework

Robust validation ensures data accuracy:

1. **Deduplication verification**: Identifies and manages duplicate records
2. **Null value detection**: Tracks missing data points for quality monitoring
3. **Metrics validation**: Ensures consistency across all stock metrics
4. **Unit testing**: Validates transformation logic with pytest and chispa

## Code Structure

```
stocks-analysis/
├── include/
│   └── phoenix/
│       ├── aws_secret_manager.py       # AWS secret management
│       ├── upload_to_s3.py             # S3 upload utilities
│       ├── glue_job_submission.py      # Glue job management
│       ├── glue_job_runner.py          # Pipeline orchestration
│       └── scripts/
│           ├── driver_rest_api_example.py        # Ticker collection
│           ├── executor_rest_api_example.py      # Price data collection
│           ├── list_based_storage_example.py     # Monthly aggregation
│           └── unit-testing-example/             # Test framework
│               ├── src/
│               │   ├── jobs/
│               │   │   └── stocks_monthly_testing_job.py
│               │   └── tests/
│               │       └── test_stocks_monthly_testing.py
├── requirements.txt                    # Project dependencies
└── README.md                           # Project documentation
```

## Technologies Used

- **Apache Spark**: Distributed processing framework
- **AWS Glue**: Serverless ETL service
- **Apache Iceberg**: Table format for large analytical datasets
- **Polygon API**: Financial market data source
- **Python**: Primary programming language
- **pytest & chispa**: Testing framework

## Getting Started

### Prerequisites

- AWS account with appropriate permissions
- Python 3.8+
- Apache Spark environment
- Polygon API key

### Configuration

1. Set up environment variables:
```bash
export SCHEMA=your_schema_name
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export POLYGON_API_KEY=your_polygon_api_key
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

### Running the Pipeline

To execute the data pipeline:

```python
from include.phoenix.glue_job_runner import create_and_run_glue_job

# Fetch stock tickers
create_and_run_glue_job('stock_tickers_job',
                       script_path='include/phoenix/scripts/driver_rest_api_example.py',
                       arguments={'--ds': '2023-08-01', '--output_table': 'schema.stock_tickers'})

# Process daily stock prices
create_and_run_glue_job('stock_prices_job',
                       script_path='include/phoenix/scripts/executor_rest_api_example.py',
                       arguments={'--ds': '2023-08-01', '--output_table': 'schema.stock_prices'})

# Generate monthly aggregations
create_and_run_glue_job('monthly_aggregation_job',
                       script_path='include/phoenix/scripts/list_based_storage_example.py',
                       arguments={'--ds': '2023-08-01', '--output_table': 'schema.monthly_stock_prices'})
```

## Data Quality Testing

Run unit tests to validate data quality and transformation logic:

```bash
cd include/phoenix/scripts/unit-testing-example
python -m pytest
```

## Data Schema

![Data Model](https://fakeimg.pl/800x200/232F3E/ffffff?text=Stock+Analysis+Data+Model)

### Primary Tables

- **stock_tickers**: Basic ticker information
- **stock_prices**: Daily price data (open, high, low, close, volume)
- **monthly_stock_prices**: Monthly aggregated metrics using array-based storage

## Future Enhancements

- Streaming data integration for real-time updates
- Machine learning models for price prediction
- Additional financial metrics and technical indicators
- Interactive visualization dashboard
- Anomaly detection for unusual market activity
