#!/bin/bash
set -e

echo "Running Beam BigQuery ETL Pipeline"

# Load environment variables
source .env

# Run the pipeline
python src/beam_pipeline.py

echo "Pipeline execution completed"