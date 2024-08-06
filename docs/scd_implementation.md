# Slowly Changing Dimensions (SCD) Type 2 Implementation

## Overview

Slowly Changing Dimensions (SCD) Type 2 is a method of tracking historical changes in dimension attributes. In our implementation, we're tracking changes in customer status over time.

## Key Design Feature: Configurable SCD Fields

A core feature of this implementation is the abstraction of SCD Type 2 field definition to the configuration layer. This design choice offers several benefits:

1. Flexibility: SCD Type 2 fields can be added, removed, or modified without changing the pipeline code.
2. Separation of Concerns: Business rules (which fields should be treated as SCD Type 2) are separated from the technical implementation.
3. Ease of Maintenance: Changes to SCD tracking can be made by updating the configuration file, reducing the risk of introducing bugs in the pipeline logic.

## Configuration

SCD Type 2 fields are defined in `config/pipeline_config.yaml`:

```yaml
scd_config:
  type2_fields:
    - customer_status
  effective_date_field: date
```

To add or remove SCD Type 2 fields, simply modify the type2_fields list in this configuration file.

## Implementation Details

The pipeline code is designed to be agnostic to the specific fields being tracked for SCD Type 2:

1. The configuration is loaded at the start of the pipeline run.
2. The ProcessChangesFn class is initialized with the list of SCD Type 2 fields from the configuration.
3. The determine_scd_type2_changes function checks for changes in any of the specified SCD Type 2 fields.

This design allows the same pipeline code to handle different SCD Type 2 scenarios without modification.

## Usage

To add more SCD Type 2 fields, simply add them to the `type2_fields` list in the `scd_config` section of `pipeline_config.yaml`.

## Querying Historical Data

To query the data at a specific point in time, use a query like:

```sql
SELECT *
FROM `project.dataset.transactions_output`
WHERE date <= '2023-08-01'
  AND (effective_to > '2023-08-01' OR effective_to IS NULL)