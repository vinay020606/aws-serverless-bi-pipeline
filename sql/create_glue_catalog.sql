-- ============================================================
--  create_glue_catalog.sql
--  Creates the AWS Glue Data Catalog table (Athena external table)
--  pointing to the S3 bucket where Lambda exports DynamoDB data.
--
--  Run this once inside the Athena Query Editor.
--  Replace <YOUR_ACCOUNT_ID> and <YOUR_BUCKET_NAME> before running.
-- ============================================================

-- Step 1: Create a dedicated database (if not already present)
CREATE DATABASE IF NOT EXISTS bi_pipeline_db
COMMENT 'Database for the AWS Serverless BI Pipeline project';


-- Step 2: Create external table that reads Parquet files from S3
--         (Lambda writes exported DynamoDB data here as JSON or Parquet)
CREATE EXTERNAL TABLE IF NOT EXISTS bi_pipeline_db.orders (
    order_id      STRING   COMMENT 'Unique order identifier',
    customer_id   STRING   COMMENT 'Customer identifier',
    customer_name STRING   COMMENT 'Full name of the customer',
    product_id    STRING   COMMENT 'Product SKU or identifier',
    product_name  STRING   COMMENT 'Human-readable product name',
    category      STRING   COMMENT 'Product category',
    quantity      INT      COMMENT 'Units purchased',
    unit_price    DOUBLE   COMMENT 'Price per unit in USD',
    total_price   DOUBLE   COMMENT 'quantity × unit_price',
    order_date    STRING   COMMENT 'ISO-8601 date string (YYYY-MM-DD)',
    region        STRING   COMMENT 'Shipping region (e.g. US-East)',
    status        STRING   COMMENT 'Order status: Delivered | Shipped | Processing | Returned'
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1',
    'ignore.malformed.json' = 'TRUE'
)
STORED AS INPUTFORMAT  'org.apache.hadoop.mapred.TextInputFormat'
         OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://<YOUR_BUCKET_NAME>/orders/'
TBLPROPERTIES (
    'has_encrypted_data'='false',
    'classification'='json'
);


-- Step 3: Verify the table was created
-- SHOW TABLES IN bi_pipeline_db;
-- SELECT * FROM bi_pipeline_db.orders LIMIT 5;

-- ============================================================
-- NOTE FOR PARQUET STORAGE (recommended for large datasets):
-- If Lambda writes Parquet instead of JSON, replace the SerDe with:
--
-- ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
-- STORED AS INPUTFORMAT  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
--          OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
-- ============================================================
