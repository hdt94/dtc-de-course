resource "google_service_account" "prefect_sa" {
  account_id   = "prefect-sa"
  display_name = "Prefect Service Account"
}

output "prefect_sa" {
  value = google_service_account.prefect_sa.email
}
