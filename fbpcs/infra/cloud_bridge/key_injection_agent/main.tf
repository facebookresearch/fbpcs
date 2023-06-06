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

data "archive_file" "zip_lambda" {
  type        = "zip"
  source_dir = "kia_source_code"
  output_path = "kia.zip"
}

resource "aws_s3_bucket_object" "upload_lambda" {
  bucket = var.kia_lambda_s3_bucket
  key    = var.kia_lambda_s3_key
  source = "kia.zip"
}

locals {
  kia_lambda_log_group       = "/aws/lambda/${var.kia_lambda_function_name}-${var.tag_postfix}"
  kia_lambda_stream_name = "kia-lambda-log-stream"
}

resource "aws_cloudwatch_log_group" "kia-lambda-log-group" {
  name = local.kia_lambda_log_group
}

resource "aws_cloudwatch_log_stream" "kia-lambda-log-stream" {
  name           = local.kia_lambda_stream_name
  log_group_name = aws_cloudwatch_log_group.kia-lambda-log-group.name
}

resource "aws_iam_role_policy" "kia_lambda_s3_access_policy" {
  name = "kia_lambda_s3_access_policy"
  role = aws_iam_role.kia_lambda_iam.name
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowLambdaAccessToS3",
      "Effect": "Allow",
      "Action": [
         "s3:*"
      ],
      "Resource":[
        "arn:aws:s3:::${var.kia_lambda_input_bucket}",
        "arn:aws:s3:::${var.kia_lambda_input_bucket}/*"
      ]
    },
    {
      "Sid": "AllowLambdaAccessToKMSKey",
      "Effect": "Allow",
      "Action": [
         "kms:CreateKey",
         "kms:CreateAlias",
         "kms:GenerateDataKey",
         "kms:TagResource"
      ],
      "Resource": "*"
    }
  ]
}
EOF
}
resource "aws_iam_role" "kia_lambda_iam" {
  name = "kia_lambda-iam${var.tag_postfix}"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_lambda_function" "kia_lambda" {
  function_name    = var.kia_lambda_function_name
  role             = aws_iam_role.kia_lambda_iam.arn
  handler          = "kia_runner.lambda_handler"
  runtime          = "python3.9"
  s3_bucket        = var.kia_lambda_s3_bucket
  s3_key           = var.kia_lambda_s3_key
  publish          = true
  environment {
    variables = {
      DEBUG = "false"
    }
  }
}
