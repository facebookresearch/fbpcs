variable "aws_region" {
  description = "region of the aws resources"
  default     = "us-west-2"
}

variable "route_table_id" {
  description = "The id of the route table"
}

variable "destination_cidr_block" {
  description = "The CIDR block of the peer VPC"
}

variable "vpc_peering_connection_id" {
  description = "The id of the VPC peering connection"
}
