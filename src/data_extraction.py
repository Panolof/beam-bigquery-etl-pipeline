import os
import pandas as pd
from google.cloud import bigquery
from utils.bigquery_helpers import get_client
import logging

logger = logging.getLogger(__name__)

def extract_data():
    logger.info("Starting data extraction")
    client = get_client()
    query = """
    SELECT *
    FROM `{}.{}.{}`
    """.format(os.getenv('PROJECT_ID'), os.getenv('DATASET_ID'), os.getenv('TABLE_ID'))
    
    df = client.query(query).to_dataframe()
    logger.info(f"Extracted {len(df)} rows")
    return df

if __name__ == "__main__":
    result = extract_data()
    print(result.head())