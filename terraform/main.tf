provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "temp_bucket" {
  name     = "${var.project_id}-temp"
  location = var.region
}

resource "google_bigquery_dataset" "pipeline_dataset" {
  dataset_id = var.dataset_id
  location   = var.region
}

resource "google_bigquery_table" "input_table" {
  dataset_id = google_bigquery_dataset.pipeline_dataset.dataset_id
  table_id   = var.input_table_id

  schema = file("${path.module}/../schema/transaction_schema.json")
}

resource "google_bigquery_table" "output_table" {
  dataset_id = google_bigquery_dataset.pipeline_dataset.dataset_id
  table_id   = var.output_table_id

  schema = file("${path.module}/../schema/transaction_schema.json")
}