resource "google_compute_network" "main" {
  name                    = "onedocker-vpc-${var.gcp_region}-${var.name_postfix}"
  auto_create_subnetworks = false
  routing_mode            = "REGIONAL"
}

resource "google_compute_subnetwork" "subnet" {
  name          = "onedocker-subnet-${var.gcp_region}-${var.name_postfix}"
  ip_cidr_range = var.subnet_primary_cidr
  region        = var.gcp_region
  network       = google_compute_network.main.id

  secondary_ip_range = [
    {
      range_name    = "pods"
      ip_cidr_range = var.subnet_secondary_cidr
    }
  ]
}

# GCP has two implied rules for all networks with lowest priority, one is let any instance send traffic to any destination
# and the other one is to block any incoming connections.
# Reference: https://cloud.google.com/vpc/docs/firewalls#default_firewall_rules
resource "google_compute_firewall" "ingress_rules" {
  name          = "onedocker-vpc-firewall-ingress-${var.gcp_region}-${var.name_postfix}"
  network       = google_compute_network.main.id
  description   = "Creates firewall ingress rule for VPCs"
  source_ranges = [var.otherparty_subnet_cidr]
  direction     = "INGRESS"
  allow {
    protocol = "tcp"
    ports    = ["5000-15500"]
  }
}
