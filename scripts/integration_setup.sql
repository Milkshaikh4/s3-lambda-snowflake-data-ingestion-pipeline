-- This script should be run AFTER the Storage Integration is created
-- and BEFORE configuring the AWS IAM Trust Relationship.

-- 1. Grant usage on the integration to the Lambda role
-- This allows the Lambda's role to actually use the integration to access S3.
GRANT USAGE ON INTEGRATION s3_integration TO ROLE LAMBDA_INGEST_ROLE;

-- 2. DESCRIBE the integration to get the values needed for the AWS Trust Relationship
-- Look for STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID in the results.
-- These must be pasted into your AWS IAM Role's Trust Policy.
DESC INTEGRATION s3_integration;
