import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from utils.data_transformations import generate_filtered_transactions, aggregate_transactions
from utils.bigquery_helpers import load_schema
import yaml
import os
import logging
from datetime import datetime 

# Set up logging
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from YAML file."""
    try:
        with open('config/pipeline_config.yaml', 'r') as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        raise

def determine_scd_type2_changes(current_record, new_record, type2_fields):
    """
    Determine if there are any changes that require a Type 2 SCD update.
    """
    for field in type2_fields:
        if current_record.get(field) != new_record.get(field):
            return True
    return False

class DeltaLoadDoFn(beam.DoFn):
    def process(self, element, last_modified_timestamp):
        # Process only records modified after the last load timestamp
        if element['last_modified_timestamp'] > last_modified_timestamp:
            yield element

def run_pipeline(config):
    logger.info("Starting pipeline")
    
    try:
        options = PipelineOptions.from_dictionary(config['beam_pipeline'])
        schema = load_schema('schema/transaction_schema.json')
        
        with beam.Pipeline(options=options) as p:
            # Read the current state of the table
            current_state = (p 
                | 'ReadCurrentState' >> beam.io.ReadFromBigQuery(
                    query=f"SELECT * FROM `{config['bigquery']['project_id']}.{config['bigquery']['dataset_id']}.{config['bigquery']['output_table_id']}` WHERE is_active = TRUE",
                    use_standard_sql=True)
                | 'KeyByTransactionId' >> beam.Map(lambda row: (row['transaction_id'], row)))

            # Read new data
            new_data = (p 
                | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(
                    query=f"SELECT * FROM `{config['bigquery']['project_id']}.{config['bigquery']['dataset_id']}.{config['bigquery']['input_table_id']}`",
                    use_standard_sql=True)
                | 'FilterTransactions' >> beam.FlatMap(lambda transactions: generate_filtered_transactions(transactions, 
                                                                                   config['data_processing']['min_transaction_amount'],
                                                                                   config['data_processing']['min_transaction_year']))
                | 'KeyByTransactionId' >> beam.Map(lambda row: (row['transaction_id'], row)))

            # Combine current state and new data
            combined_data = ((current_state, new_data) 
                | 'GroupByKey' >> beam.CoGroupByKey()
                | 'ProcessChanges' >> beam.ParDo(ProcessChangesFn(config['scd_config']['type2_fields'])))

            # Write results back to BigQuery
            combined_data | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                f"{config['bigquery']['project_id']}:{config['bigquery']['dataset_id']}.{config['bigquery']['output_table_id']}",
                schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)

    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise

class ProcessChangesFn(beam.DoFn):
    def __init__(self, type2_fields):
        self.type2_fields = type2_fields

    def process(self, element):
        transaction_id, data = element
        current_records = list(data[0])
        new_records = list(data[1])

        if not current_records and new_records:
            # New record
            new_record = new_records[0]
            new_record['is_active'] = True
            new_record['effective_from'] = datetime.now().date()
            yield new_record
        elif current_records and new_records:
            # Existing record, check for changes
            current_record = current_records[0]
            new_record = new_records[0]
            if determine_scd_type2_changes(current_record, new_record, self.type2_fields):
                # SCD Type 2 change
                current_record['is_active'] = False
                current_record['effective_to'] = datetime.now().date()
                yield current_record

                new_record['is_active'] = True
                new_record['effective_from'] = datetime.now().date()
                yield new_record
            else:
                # No change or SCD Type 1 change
                current_record.update(new_record)
                yield current_record

if __name__ == "__main__":
    try:
        config = load_config()
        run_pipeline(config)
        logger.info("Pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")