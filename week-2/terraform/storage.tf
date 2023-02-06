resource "google_storage_bucket" "data-lake-bucket" {
  name     = var.gcs_data_lake_bucket
  location = var.region

  # Optional, but recommended settings:
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 7 // days
    }
  }

  force_destroy = true
}

resource "google_storage_bucket_iam_member" "member" {
  bucket = google_storage_bucket.data-lake-bucket.name
  role   = "roles/storage.admin"
  member = google_service_account.prefect_sa.member
}
