terraform {
   required_version = ">= 1.0"
   backend "local" {} # Options [local, gcs, s3] 1 and 2 preserve tf-state online
   required_providers {
     google = {
        source = "hashicorp/google"
     }
   }
}

provider "google" {
    project = var.project
    region = var.region
}

# Data Lake bucket
resource "google_storage_bucket" "data-lake-bucket" {
    name = "${local.data_lake_bucket}_${var.project}" # Concatenates Bucket & Project name
    location = var.region

    storage_class = var.storage_class
    uniform_bucket_level_access = true

    versioning {
        enabled = true
    }

    lifecycle_rule {
        action{
            type = "Delete"
        }
        condition {
            age = 30 # Days
        }
    }

    force_destroy =  true
}

# Data WareHouse
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.BQ_DATASET
  project = var.project
  location = var.region
}