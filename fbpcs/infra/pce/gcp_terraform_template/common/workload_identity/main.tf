provider "google" {
  project     = var.project_id
}

terraform {
  backend "gcs" {
  }
}

provider "kubernetes" {
  config_path    = "~/.kube/config"
  config_context = "gke_${var.project_id}_${var.gcp_region}_${var.cluster_name}"
}
