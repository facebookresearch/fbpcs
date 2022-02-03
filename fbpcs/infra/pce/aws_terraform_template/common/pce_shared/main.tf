provider "aws" {
  profile = "default"
  region  = var.aws_region
}

terraform {
  backend "s3" {}
}
