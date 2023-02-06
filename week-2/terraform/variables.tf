variable "bq_dataset" {
  type = string
}

variable "gcs_data_lake_bucket" {
  type = string
}

variable "project" {
  description = "Your GCP Project ID"
  type        = string
}

variable "region" {
  default = "us-central1"
  type    = string
}
