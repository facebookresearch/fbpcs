provider "aws" {
  profile = "default"
  region  = var.aws_region
  default_tags {
    tags = {
      "pce:pce-id" = var.pce_id
    }
  }
}

terraform {
  backend "s3" {}
}
