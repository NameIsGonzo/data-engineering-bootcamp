locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
    description = "dtc-data-eng-course"
}

variable "region" {
  description = "Region for GCP Resources"
  default = "us-west1"
  type = string
}

variable "storage_class" {
  description = "Storage class type for our buckets"
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data from GCS will be written to"
  type = string
  default = "trips_data_all"
}
