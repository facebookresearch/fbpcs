provider "aws" {
  profile = "default"
  region  = var.aws_region
}

terraform {
  backend "s3" {}
}

resource "aws_vpc_peering_connection" "vpc_peering_conn" {
  peer_owner_id = var.peer_aws_account_id
  peer_vpc_id   = var.peer_vpc_id
  vpc_id        = var.vpc_id

  tags = {
    Name = "onedocker-vpc-peering${var.tag_postfix}"
  }
}
