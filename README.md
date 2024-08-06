# Beam BigQuery ETL Pipeline with SCD Type 2 Support

This project demonstrates a data processing pipeline using Apache Beam and Google BigQuery, with support for Slowly Changing Dimensions (SCD) Type 2.

## Features

- Data extraction from BigQuery
- Transaction filtering based on amount and year
- Configurable SCD Type 2 implementation
  - SCD Type 2 fields are defined in the configuration, allowing for easy modification without changing the pipeline code
  - Current implementation tracks historical changes in customer status
- Data aggregation and processing using Apache Beam
- Results writing back to BigQuery

## SCD Type 2 Implementation

This pipeline implements SCD Type 2 for fields specified in the configuration. By default, it's set up for the `customer_status` field. This allows us to track historical changes in a customer's status over time.

Key points about the SCD Type 2 implementation:

1. SCD Type 2 fields are defined in `config/pipeline_config.yaml`, not hardcoded in the pipeline.
2. Adding or removing SCD Type 2 fields only requires updating the configuration, not the pipeline code.
3. This abstraction allows for flexible business rules without code changes.

When a tracked field changes (e.g., customer status from 'Silver' to 'Gold'), the pipeline will:

1. Close the current record by setting `is_active` to `False` and `effective_to` to the current date.
2. Create a new record with the updated status, setting `is_active` to `True` and `effective_from` to the current date.

This approach allows for historical analysis of customer statuses and their impact on transactions.

For more details on the implementation and how to modify SCD Type 2 fields, see `docs/scd_implementation.md`.

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

## Testing

This project includes a comprehensive test suite to ensure the correct functioning of the pipeline, including the SCD Type 2 implementation.

### Running Tests

To run the full test suite:

```bash
python -m unittest discover tests
```

### Test Structure

The tests are designed to be configuration-driven, mirroring the flexibility of the main pipeline:

1. **SCD Implementation Tests** (`tests/test_scd_implementation.py`):
   - These tests use the same configuration as the main pipeline, loaded from `config/pipeline_config.yaml`.
   - They automatically adapt to changes in SCD field configuration, ensuring consistency between tests and actual pipeline behavior.
   - Key test cases include:
     - Verifying correct identification of SCD Type 2 changes
     - Ensuring non-SCD field changes don't trigger SCD updates
     - Checking proper creation of new records and closing of old records in SCD scenarios

2. **Data Transformation Tests** (`tests/test_data_transformations.py`):
   - These tests cover the core data processing functions, including transaction filtering and aggregation.

### Writing New Tests

When adding new features or modifying existing ones:

1. Ensure new tests are added to cover the changes.
2. For SCD-related changes, modify `test_scd_implementation.py` as needed, always using the configuration-driven approach.
3. Run the full test suite to ensure no regressions.

The test suite is crucial for maintaining the reliability and flexibility of the pipeline. Always ensure all tests pass before submitting a pull request.

## Continuous Integration

This project uses GitHub Actions for continuous integration. The workflow is defined in `.github/workflows/main.yml`. On each push and pull request:

- The test suite is run
- Linting is performed using pylint

To run these checks locally:

1. Run tests: `python -m unittest discover tests`
2. Run linting: `pylint src tests`

Note: The CI workflow can be found in the `.github/workflows/main.yml` file. You can modify this file to adjust the CI process as needed.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE.txt) file for details.