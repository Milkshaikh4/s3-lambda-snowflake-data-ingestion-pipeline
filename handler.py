import os
import json
import logging
from boto3.session import Session 
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_private_key():
    """Fetches the Snowflake private key from AWS Secrets Manager."""
    secret_name = os.environ['SNOWFLAKE_PRIVATE_KEY_SECRET']
    region_name = os.environ.get('AWS_REGION', 'ap-southeast-2')

    # Create a Secrets Manager client
    session = Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as e:
        logger.error(f"Error retrieving secret {secret_name}: {str(e)}")
        raise e

    # The secret is stored as a plaintext string
    private_key_pem = get_secret_value_response['SecretString']
    
    # Convert PEM to DER format (which the Snowflake connector expects)
    p_key = serialization.load_pem_private_key(
        private_key_pem.encode(),
        password=None, # Or your private key's passphrase if encrypted
        backend=default_backend()
    )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    return pkb

def get_snowflake_connection():
    """Establishes a connection to Snowflake using Key Pair Auth."""
    pkb = get_private_key()
    
    return snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        private_key=pkb,
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database=os.environ['SNOWFLAKE_DATABASE'],
        schema=os.environ['SNOWFLAKE_SCHEMA'],
        role=os.environ.get('SNOWFLAKE_ROLE')
    )

def ingest_data(event, context):
    """
    Lambda handler triggered by S3 event.
    Triggers a Snowflake Notebook (Workbook) to handle ingestion.
    """
    logger.info(f"Received S3 event: {json.dumps(event)}")
    
    notebook_name = os.environ.get('SNOWFLAKE_NOTEBOOK_NAME')
    if not notebook_name:
        logger.error("SNOWFLAKE_NOTEBOOK_NAME environment variable is not set.")
        return {'statusCode': 500, 'body': 'Configuration error.'}

    try:
        # 1. Parse S3 Event
        for record in event['Records']:
            bucket_name = record['s3']['bucket']['name']
            file_key = record['s3']['object']['key']
            
            logger.info(f"Triggering notebook {notebook_name} for file: s3://{bucket_name}/{file_key}")
            
            # 2. Trigger Snowflake Notebook
            conn = get_snowflake_connection()
            cur = conn.cursor()
            
            try:
                # EXECUTE NOTEBOOK requires the fully qualified name or IDENTIFIER
                # We also pass the bucket and key as arguments to the notebook.
                # Inside the notebook, access these via: import sys; bucket = sys.argv[1]; key = sys.argv[2]
                
                query = f"EXECUTE NOTEBOOK {notebook_name}('{bucket_name}', '{file_key}');"
                logger.info(f"Executing: {query}")
                
                cur.execute(query)
                
                logger.info(f"Notebook {notebook_name} triggered successfully.")
                
            finally:
                cur.close()
                conn.close()
                
        return {
            'statusCode': 200,
            'body': json.dumps({'message': f'Notebook {notebook_name} triggered.'})
        }

    except Exception as e:
        logger.error(f"Error triggering notebook: {str(e)}")
        raise e
