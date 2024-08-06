import os
import pandas as pd
from google.cloud import bigquery
from utils.bigquery_helpers import get_client, load_schema
import logging
import yaml

logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from YAML file."""
    try:
        with open('config/pipeline_config.yaml', 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        raise

def extract_data(config):
    """
    Extract data from BigQuery table based on the provided configuration.
    
    Args:
        config (dict): Configuration dictionary containing BigQuery details.
    
    Returns:
        pandas.DataFrame: Extracted data as a DataFrame.
    """
    logger.info("Starting data extraction")
    
    # Get an authenticated BigQuery client
    client = get_client()
    
    # Load the schema to ensure we're extracting all necessary fields
    schema = load_schema('schema/transaction_schema.json')
    fields = ', '.join([field['name'] for field in schema])
    
    # Construct the SQL query using configuration values
    query = f"""
    SELECT {fields}
    FROM `{config['bigquery']['project_id']}.{config['bigquery']['dataset_id']}.{config['bigquery']['input_table_id']}`
    """
    
    # Execute the query and load results into a DataFrame
    df = client.query(query).to_dataframe()
    
    logger.info(f"Extracted {len(df)} rows")
    return df

if __name__ == "__main__":
    # Load the configuration
    config = load_config()
    
    # Extract the data
    result = extract_data(config)
    
    # Print the first few rows of the result
    print(result.head())
    
    # Log the column names to verify all required fields are present
    logger.info(f"Extracted columns: {result.columns.tolist()}")