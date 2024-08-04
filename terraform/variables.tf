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
