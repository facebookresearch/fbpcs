provider "aws" {
  profile = "default"
  region  = var.region
}

provider "archive" {}

terraform {
  backend "s3" {}
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

resource "aws_s3_bucket_notification" "semi_auto_bucket_notification" {
  bucket = var.data_bucket_name
  lambda_function {
    lambda_function_arn = var.semi_automated_lambda_arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "${var.data_upload_key_path}/"
    filter_suffix       = ".csv"
  }
}
