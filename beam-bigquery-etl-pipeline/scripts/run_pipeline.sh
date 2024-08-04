#!/bin/bash
set -e

echo "Running Beam BigQuery ETL Pipeline"
python src/beam_pipeline.py
