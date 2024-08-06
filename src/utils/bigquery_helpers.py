from google.cloud import bigquery
from google.oauth2 import service_account
import os
import json
import logging

logger = logging.getLogger(__name__)

def load_schema(schema_file):
    with open(schema_file, 'r') as f:
        schema = json.load(f)
    return schema['fields']

def get_client():
    """
    Create and return an authenticated BigQuery client.
    
    Returns:
        google.cloud.bigquery.client.Client: An authenticated BigQuery client.
    
    Raises:
        Exception: If unable to create the BigQuery client.
    """
    try:
        credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        project_id = os.getenv('PROJECT_ID')
        
        if not credentials_path or not project_id:
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS and PROJECT_ID must be set in environment variables")
        
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        
        client = bigquery.Client(credentials=credentials, project=project_id)
        logger.info(f"BigQuery client created successfully for project: {project_id}")
        return client
    except Exception as e:
        logger.error(f"Failed to create BigQuery client: {str(e)}")
        raise