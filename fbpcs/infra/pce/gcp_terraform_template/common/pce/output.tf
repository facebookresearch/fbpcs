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

output "gke_service_account" {
  value       = google_service_account.gke_sa.email
  description = "The google service account used in GKE"
}

output "k8s_namespace" {
  value       = kubernetes_namespace.k8s_ns.metadata[0].name
  description = "kubernetes namespace"
}

output "k8s_service_account" {
  value       = kubernetes_service_account.k8s_sa.metadata[0].name
  description = "The name of provisioned kubernetes service account"
}
