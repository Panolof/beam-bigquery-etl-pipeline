variable "project_id" {
  description = "The GCP project ID"
}

variable "region" {
  description = "The GCP region"
  default     = "europe-west2"  # This is the London region
}

variable "dataset_id" {
  description = "The BigQuery dataset ID"
}

variable "input_table_id" {
  description = "The BigQuery input table ID"
}

variable "output_table_id" {
  description = "The BigQuery output table ID"
}