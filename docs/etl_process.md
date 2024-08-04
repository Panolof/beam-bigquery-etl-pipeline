# ETL Process Documentation

This document outlines the Extract, Transform, Load (ETL) process implemented in our Beam BigQuery pipeline.

## Extract

Data is extracted from a BigQuery table specified in the configuration. The extraction process is handled by the `extract_data()` function in `src/data_extraction.py`.

### Process:
1. Connect to BigQuery using authenticated client.
2. Execute a SQL query to fetch all data from the specified table.
3. Load the query results into a pandas DataFrame.

## Transform

Data transformation is performed using Apache Beam's parallel processing capabilities. The main transformations are defined in `src/utils/data_transformations.py`.

### Transformations:
1. **Filtering**: Transactions are filtered based on:
   - Minimum transaction amount
   - Minimum transaction year
2. **Aggregation**: Transactions are aggregated by date, summing the total amount for each date.

## Load

The transformed and aggregated data is loaded back into a BigQuery table. This process is handled within the Beam pipeline defined in `src/beam_pipeline.py`.

### Process:
1. Beam writes the processed data to a specified BigQuery table.
2. If the table doesn't exist, it's created with an auto-detected schema.
3. If the table exists, its content is overwritten with the new data.

## Error Handling and Logging

Throughout the ETL process, comprehensive error handling and logging are implemented to ensure robustness and ease of debugging.