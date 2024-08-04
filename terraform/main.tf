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
