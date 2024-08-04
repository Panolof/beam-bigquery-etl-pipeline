import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from utils.data_transformations import filter_transactions, aggregate_transactions
import yaml
import os
import logging

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

def load_config():
    try:
        with open('config/pipeline_config.yaml', 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        raise

def run_pipeline(config):
    logger.info("Starting pipeline")
    
    try:
        options = PipelineOptions.from_dictionary(config['beam_pipeline'])
        
        with beam.Pipeline(options=options) as p:
            (p 
             | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(
                 query=f"SELECT * FROM `{config['bigquery']['project_id']}.{config['bigquery']['dataset_id']}.{config['bigquery']['input_table_id']}`",
                 use_standard_sql=True)
             | 'FilterTransactions' >> beam.Map(lambda row: filter_transactions(row, 
                                                                               config['data_processing']['min_transaction_amount'],
                                                                               config['data_processing']['min_transaction_year']))
             | 'RemoveNone' >> beam.Filter(lambda x: x is not None)
             | 'AggregateTransactions' >> beam.CombineGlobally(aggregate_transactions)
             | 'FlattenResults' >> beam.FlatMap(lambda x: x)
             | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                 f"{config['bigquery']['project_id']}:{config['bigquery']['dataset_id']}.{config['bigquery']['output_table_id']}",
                 schema='SCHEMA_AUTODETECT',
                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
            )
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        config = load_config()
        run_pipeline(config)
        logger.info("Pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")