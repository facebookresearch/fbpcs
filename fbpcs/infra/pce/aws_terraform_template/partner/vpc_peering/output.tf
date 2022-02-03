output "vpc_peering_connection_id" {
  value       = aws_vpc_peering_connection.vpc_peering_conn.id
  description = "The id of the VPC peering connection"
}
