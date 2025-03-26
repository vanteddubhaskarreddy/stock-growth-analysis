import datetime
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, udf, concat, from_json
from pyspark.sql.types import StringType,StructType,StructField,IntegerType,DoubleType, LongType


spark = (SparkSession.builder.getOrCreate())
args = getResolvedOptions(sys.argv, ["JOB_NAME", "ds", 'output_table'])
run_date = args['ds']
output_table = args['output_table']

glueContext = GlueContext(spark.sparkContext)
spark = glueContext.spark_session

spark.sql("""
CREATE TABLE IF NOT EXISTS bhaskar_reddy07.monthly_stock_prices (
  symbol STRING,
  from_date STRING,
  metric_name STRING,
  metric_array ARRAY<DOUBLE>,
  month_start STRING
)
USING iceberg
PARTITIONED BY (symbol, month_start, metric_name)
""")


spark.sql("""
INSERT INTO bhaskar_reddy07.monthly_stock_prices
SELECT 
  symbol,
  from AS from_date,
  'high' AS metric_name,
  ARRAY(high) AS metric_array,
  DATE_FORMAT(DATE(from), 'yyyy-MM-01') AS month_start
FROM bootcamp.stock_prices
WHERE DATE_FORMAT(DATE(from), 'yyyy-MM-dd') = '2023-08-01'

UNION ALL

SELECT 
  symbol,
  from AS from_date,
  'low' AS metric_name,
  ARRAY(low) AS metric_array,
  DATE_FORMAT(DATE(from), 'yyyy-MM-01') AS month_start
FROM bootcamp.stock_prices
WHERE DATE_FORMAT(DATE(from), 'yyyy-MM-dd') = '2023-08-01'

UNION ALL

SELECT 
  symbol,
  from AS from_date,
  'close' AS metric_name,
  ARRAY(close) AS metric_array,
  DATE_FORMAT(DATE(from), 'yyyy-MM-01') AS month_start
FROM bootcamp.stock_prices
WHERE DATE_FORMAT(DATE(from), 'yyyy-MM-dd') = '2023-08-01'
""")


def update_monthly_stock_data(update_date):
    date_obj = datetime.strptime(update_date, '%Y-%m-%d')
    month_start = date_obj.strftime('%Y-%m-01')
    day_of_month = date_obj.day
    
    update_sql = f"""
    INSERT INTO bhaskar_reddy07.monthly_stock_prices
    WITH yesterday AS (
      SELECT * FROM bhaskar_reddy07.monthly_stock_prices
      WHERE month_start = '{month_start}'
    ),
    today AS (
      SELECT 
        symbol,
        from AS from_date,
        'high' AS metric_name,
        high AS metric_value,
        '{month_start}' AS month_start
      FROM bootcamp.stock_prices
      WHERE DATE_FORMAT(DATE(from), 'yyyy-MM-dd') = '{update_date}'
      
      UNION ALL
      
      SELECT 
        symbol,
        from AS from_date,
        'low' AS metric_name,
        low AS metric_value,
        '{month_start}' AS month_start
      FROM bootcamp.stock_prices
      WHERE DATE_FORMAT(DATE(from), 'yyyy-MM-dd') = '{update_date}'
      
      UNION ALL
      
      SELECT 
        symbol,
        from AS from_date,
        'close' AS metric_name,
        close AS metric_value,
        '{month_start}' AS month_start
      FROM bootcamp.stock_prices
      WHERE DATE_FORMAT(DATE(from), 'yyyy-MM-dd') = '{update_date}'
    )
    SELECT 
      COALESCE(t.symbol, y.symbol) AS symbol,
      t.from_date AS from_date,
      COALESCE(t.metric_name, y.metric_name) AS metric_name,
      
      -- Build array with previous values and append new value
      -- Filling with nulls if needed to position new value at the correct day-of-month index
      CASE 
        WHEN y.metric_array IS NULL THEN 
          CONCAT(ARRAY_REPEAT(CAST(NULL AS DOUBLE), {day_of_month - 1}), ARRAY(t.metric_value))
        ELSE 
          CONCAT(y.metric_array, 
                 ARRAY_REPEAT(CAST(NULL AS DOUBLE), {day_of_month - 1} - CARDINALITY(y.metric_array)),
                 ARRAY(t.metric_value))
      END AS metric_array,
      
      '{month_start}' AS month_start
    FROM today t
    FULL OUTER JOIN yesterday y
      ON t.symbol = y.symbol AND t.metric_name = y.metric_name
    """
    
    spark.sql(update_sql)

job = Job(glueContext)
job.init(args["JOB_NAME"], args)