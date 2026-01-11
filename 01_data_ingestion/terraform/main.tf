terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.72.1"
    }
  }
}

provider "google" {
  # Configuration options
}

resource "google_storage_bucket" "primary_bucket" {
  name          = var.bucket_name
  location      = "US"
  force_destroy = true
  project       = var.project_id
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id    = var.dataset_id
  friendly_name = "dtdez"
  location      = "US"
  project       = var.project_id
}