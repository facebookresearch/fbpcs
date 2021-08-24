output "aws_ecs_cluster_name" {
  value       = aws_ecs_cluster.main.name
  description = "The name of the ecs cluster"
}

output "vpc_id" {
  value       = aws_vpc.main.id
  description = "The id of the provisioned VPC"
}

output "vpc_cidr" {
  value       = aws_vpc.main.cidr_block
  description = "The CIDR block of the provisioned VPC"
}

output "subnets" {
  value = [
    for subnet in aws_subnet.subnets : subnet.id
  ]
  description = "The subnets in the region associated with this VPC"
}

output "security_group_id" {
  value       = aws_default_security_group.default.id
  description = "The id of the default security group"
}

output "route_table_id" {
  value       = aws_vpc.main.main_route_table_id
  description = "The id of the route table"
}
