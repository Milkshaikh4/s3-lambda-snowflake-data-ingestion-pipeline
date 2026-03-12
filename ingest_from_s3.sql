-- This SQL script is executed by the Lambda whenever a new CSV file arrives in S3.
-- Arguments: {bucket_name}, {file_key} are injected by the Lambda at runtime.

-- 1. Create a temporary Stage to point to the new file
CREATE OR REPLACE TEMPORARY STAGE SNOWFLAKE_LEARNING_DB.NABILSHAIKH_LOAD_DATA_FROM_CLOUD.temp_s3_stage
  URL = 's3://{bucket_name}/{file_key}';

-- 2. Ingest the data from S3 into the table (Table must already exist!)
COPY INTO SNOWFLAKE_LEARNING_DB.NABILSHAIKH_LOAD_DATA_FROM_CLOUD.TRUCK_LOADS
FROM @SNOWFLAKE_LEARNING_DB.NABILSHAIKH_LOAD_DATA_FROM_CLOUD.temp_s3_stage
FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1);
