variable "aws_region" {
  description = "region of the aws resources"
  default     = "us-west-2"
}

variable "tag_postfix" {
  description = "the postfix to append after a resource name or tag"
  default     = ""
}

variable "peer_aws_account_id" {
  description = "accepter's aws account id"
  default     = ""
}

variable "peer_vpc_id" {
  description = "accepter's VPC id"
  default     = ""
}

variable "vpc_id" {
  description = "requester's VPC id"
  default     = ""
}

variable "route_table_id" {
  description = "The id of the route table"
  default     = ""
}

variable "destination_cidr_block" {
  description = "The CIDR block of the peer VPC"
  default     = "10.0.0.0/16"
}
