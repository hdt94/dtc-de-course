/*
References:
https://cloud.google.com/bigquery/docs/access-control
*/

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset
}

resource "google_bigquery_dataset_iam_member" "data_editor" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.prefect_sa.member
}

resource "google_project_iam_member" "project" {
  project = var.project
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.prefect_sa.member
}

resource "google_bigquery_dataset_iam_member" "user" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  role       = "roles/bigquery.user"
  member     = google_service_account.prefect_sa.member
}
