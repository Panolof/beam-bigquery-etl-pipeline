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

Add tests for any new functionality. Ensure all tests pass by running:

```
python -m unittest discover tests
```

## Documentation

Update documentation as necessary, including inline comments, README.md, and files in the `docs/` directory.

Thank you for your contribution!