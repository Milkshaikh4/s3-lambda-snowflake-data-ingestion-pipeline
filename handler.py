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
    Reads a local SQL file and executes it in Snowflake to handle ingestion.
    """
    logger.info(f"Received S3 event: {json.dumps(event)}")
    
    sql_file_path = os.environ.get('SNOWFLAKE_SQL_FILE', 'ingest_from_s3.sql')
    
    try:
        # Load the SQL template
        with open(sql_file_path, 'r') as f:
            sql_template = f.read()
            
        # 1. Parse S3 Event
        for record in event['Records']:
            bucket_name = record['s3']['bucket']['name']
            file_key = record['s3']['object']['key']
            
            logger.info(f"Ingesting file: s3://{bucket_name}/{file_key}")
            
            # 2. Establish Connection
            conn = get_snowflake_connection()
            cur = conn.cursor()
            
            try:
                # 3. Inject variables and Split SQL into individual commands
                # (Snowflake's connector handles one command at a time)
                final_sql = sql_template.format(bucket_name=bucket_name, file_key=file_key)
                commands = [cmd.strip() for cmd in final_sql.split(';') if cmd.strip()]
                
                for command in commands:
                    logger.info(f"Executing SQL command: {command[:100]}...")
                    cur.execute(command)
                
                logger.info(f"Ingestion for {file_key} complete.")
                
            finally:
                cur.close()
                conn.close()
                
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Ingestion successful.'})
        }

    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise e
