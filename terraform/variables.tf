variable "project_id" {
  description = "The GCP project ID"
}

variable "region" {
  description = "The GCP region"
  default     = "us-central1"
}

variable "dataset_id" {
  description = "The BigQuery dataset ID"
}
