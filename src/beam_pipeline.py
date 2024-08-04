import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from utils.data_transformations import filter_transactions
import yaml
import os
import logging

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

def load_config():
    with open('config/pipeline_config.yaml', 'r') as file:
        return yaml.safe_load(file)

def run_pipeline(config):
    logger.info("Starting pipeline")
    
    options = PipelineOptions.from_dictionary(config['beam_pipeline'])
    
    with beam.Pipeline(options=options) as p:
        (p 
         | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(
             query=f"SELECT * FROM \`{config['bigquery']['project_id']}.{config['bigquery']['dataset_id']}.{config['bigquery']['input_table_id']}\`",
             use_standard_sql=True)
         | 'FilterTransactions' >> beam.Map(lambda row: filter_transactions(row, 
                                                                           config['data_processing']['min_transaction_amount'],
                                                                           config['data_processing']['min_transaction_year']))
         | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
             f"{config['bigquery']['project_id']}:{config['bigquery']['dataset_id']}.{config['bigquery']['output_table_id']}",
             schema='SCHEMA_AUTODETECT',
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
             write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
        )

if __name__ == "__main__":
    config = load_config()
    run_pipeline(config)