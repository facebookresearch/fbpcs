provider "google" {
  credentials = file(var.credential_path)
  project     = var.project_id
}

terraform {
  backend "gcs" {
  }
}
