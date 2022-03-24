output "vpc_peering_id" {
  value       = google_compute_network_peering.vpc_peering.id
  description = "The id of the VPC peering connection"
}
