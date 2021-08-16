provider "aws" {
  profile = "default"
  region  = var.aws_region
}

terraform {
  backend "s3" {}
}

resource "aws_route" "peer_connection_access" {
  route_table_id            = var.route_table_id
  destination_cidr_block    = var.destination_cidr_block
  vpc_peering_connection_id = var.vpc_peering_connection_id
}
