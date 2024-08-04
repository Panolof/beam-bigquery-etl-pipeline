from google.cloud import bigquery
import os
import logging

logger = logging.getLogger(__name__)

def get_client():
    try:
        return bigquery.Client()
    except Exception as e:
        logger.error(f"Failed to create BigQuery client: {str(e)}")
        raise
