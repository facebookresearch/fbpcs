resource "google_container_cluster" "main" {
  name             = "onedocker-cluster-${var.name_postfix}"
  location         = var.gcp_region
  network          = google_compute_network.main.id
  subnetwork       = google_compute_subnetwork.subnet.id
  enable_autopilot = true

  ip_allocation_policy {
    cluster_secondary_range_name = "pods"
  }
}
