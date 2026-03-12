-- 1. Set the Context
USE DATABASE SNOWFLAKE_LEARNING_DB;
USE SCHEMA NABILSHAIKH_LOAD_DATA_FROM_CLOUD;

-- 2. Create a temporary Stage pointing to the BUCKET (folder)
CREATE OR REPLACE TEMPORARY STAGE temp_s3_stage
  URL = 's3://{bucket_name}/'
  STORAGE_INTEGRATION = s3_integration;

-- 3. Ingest only the specific FILE that triggered the Lambda
COPY INTO TRUCK_LOADS
FROM @temp_s3_stage
FILES = ('{file_key}')
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1);
