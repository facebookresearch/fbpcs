output "vpc_name" {
  value       = google_compute_network.main.name
  description = "The name of the provisioned VPC"
}

output "vpc_id" {
  value       = google_compute_network.main.id
  description = "The id of the provisioned VPC"
}

output "subnet_primary_cidr" {
  value       = google_compute_subnetwork.subnet.ip_cidr_range
  description = "The CIDR block of the provisioned subnet for compute engine"
}

output "subnet_secondary_cidr" {
  value       = google_compute_subnetwork.subnet.secondary_ip_range[0].ip_cidr_range
  description = "The CIDR block of the provisioned subnet for kubernetes engine"
}

output "subnet_id" {
  value       = google_compute_subnetwork.subnet.id
  description = "The id of the subnet associated with the vpc"
}

output "cluster_name" {
  value = google_container_cluster.main.name
}
