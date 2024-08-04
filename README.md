# Beam BigQuery ETL Pipeline

This project demonstrates a data processing pipeline using Apache Beam and Google BigQuery. It processes transaction data, filtering and aggregating based on configured criteria.

## Features

- Data extraction from BigQuery
- Transaction filtering based on amount and year
- Data aggregation and processing using Apache Beam
- Results writing back to BigQuery

## Documentation

For more detailed information, see the `docs/` directory.

## Setup

1. Clone the repository:
   ```
   git clone https://github.com/your-username/beam-bigquery-etl-pipeline.git
   cd beam-bigquery-etl-pipeline
   ```

2. Set up a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Set up your Google Cloud credentials:
   - Create a service account and download the JSON key
   - Set the GOOGLE_APPLICATION_CREDENTIALS environment variable:
     ```
     export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
     ```

5. Update the `config/pipeline_config.yaml` file with your GCP project details and BigQuery information.

## Usage

Run the pipeline using the provided script:

```
bash scripts/run_pipeline.sh
```

## Development

To contribute to this project:

1. Create a new branch for your feature
2. Make your changes and write tests
3. Run tests using `python -m unittest discover tests`
4. Submit a pull request

## Continuous Integration

This project uses GitHub Actions for continuous integration. On each push and pull request:

- The test suite is run
- Linting is performed using pylint

To run these checks locally:

1. Run tests: `python -m unittest discover tests`
2. Run linting: `pylint src tests`

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.