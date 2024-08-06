# Contributing to Beam BigQuery ETL Pipeline

We welcome contributions to this project! Here are some guidelines to follow:

## Getting Started

1. Fork the repository and create your branch from `main`.
2. Set up your development environment using the `scripts/setup_environment.sh` script.
3. Make your changes, adding tests where necessary.
4. Ensure all tests pass and the code lints correctly.
5. Submit a pull request.

## Code Style

We use:
- `black` for code formatting
- `pylint` for linting
- `isort` for import sorting

Please ensure your code adheres to these standards before submitting a pull request.

## Commit Messages

Use clear and meaningful commit messages. Each commit message should describe what changed and why.

## Testing

Our test suite, including SCD Type 2 tests, uses the same configuration as the main pipeline. This ensures that tests automatically adapt to changes in SCD field configuration. When adding or modifying tests:

1. Always use the configuration loaded from `config/pipeline_config.yaml` in your tests, rather than hardcoding field names.
2. Ensure your tests cover all SCD Type 2 fields specified in the configuration.
3. Include tests to verify that changes in non-SCD fields don't trigger SCD Type 2 updates.
4. For new features, add corresponding test cases in the appropriate test file.
5. When modifying existing functionality, update related tests and add new ones if necessary.
6. Run the full test suite using `python -m unittest discover tests` to ensure all tests pass.

Key test files:
- `tests/test_scd_implementation.py`: Tests for SCD Type 2 logic
- `tests/test_data_transformations.py`: Tests for data processing functions

This approach maintains consistency between our tests and the actual pipeline behavior, enhancing the reliability of our test suite and overall pipeline functionality.

## Documentation

Update documentation as necessary, including inline comments, README.md, and files in the `docs/` directory.

## Continuous Integration

This project uses GitHub Actions for CI. The workflow is defined in `.github/workflows/main.yml`. Please ensure your changes pass all tests and lint checks before submitting a pull request.

Note: If the CI is temporarily disabled, please run the tests and lint checks locally before submitting your PR.

## SCD Type 2 Implementation

The SCD Type 2 implementation is configuration-driven, allowing for flexible business rules without code changes. When contributing:

1. For changes to which fields are tracked as SCD Type 2, update the `type2_fields` list in `config/pipeline_config.yaml`.
2. If adding new fields to be tracked, ensure they are also added to the schema in `schema/transaction_schema.json`.
3. The core SCD logic in `beam_pipeline.py` should rarely need modification unless changing the fundamental behavior of SCD tracking.
4. Always update the documentation in `docs/scd_implementation.md` to reflect any changes in configuration or behavior.

This approach maintains the separation of configuration from implementation, enhancing the maintainability and flexibility of the pipeline.

Thank you for your contribution!