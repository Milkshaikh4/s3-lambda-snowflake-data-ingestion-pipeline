# s3-lambda-snowflake-data-ingestion-pipeline
The S3 bucket pings a lambda once data is placed into the S3 bucket. The Lambda signs a JWT to pass to snowflake. Python Lambda will handle validations. Notify Snowflake to ingest data. 
