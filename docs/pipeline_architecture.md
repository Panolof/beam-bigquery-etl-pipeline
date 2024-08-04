# Pipeline Architecture

This document describes the architecture of the Beam BigQuery ETL Pipeline.

## Overview

The pipeline is designed to process transaction data from BigQuery, filter and aggregate it, and then write the results back to BigQuery.

## Components

1. **Data Extraction**: Utilizes BigQuery to fetch raw transaction data.
2. **Data Transformation**: 
   - Filters transactions based on amount and year.
   - Aggregates transactions by date.
3. **Data Loading**: Writes processed data back to BigQuery.

## Flow

1. Configuration is loaded from `config/pipeline_config.yaml`.
2. Apache Beam pipeline is set up with the loaded configuration.
3. Data is read from the specified BigQuery table.
4. Transactions are filtered based on configured criteria.
5. Filtered transactions are aggregated by date.
6. Aggregated results are written to the output BigQuery table.

## Error Handling

The pipeline includes error handling and logging at various stages to capture and report any issues during execution.

## Scalability

The use of Apache Beam allows for scalable processing, with the ability to run on various backends including local, Google Cloud Dataflow, and Apache Flink.