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
  source_dir = "mva_source_code"
  output_path = "mva_source.zip"
}

resource "aws_s3_bucket_object" "upload_lambda" {
  bucket = var.measurement_validation_agent_lambda_source_bucket
  key    = var.measurement_validation_agent_lambda_s3_key
  source = "mva_source.zip"
}

locals {
  measurement_validation_agent_lambda_log_group       = "/aws/lambda/${var.measurement_validation_agent_lambda_function_name}"
  measurement_validation_agent_lambda_stream_name     = "measurement-validation-agent-lambda-log-stream"
}

resource "aws_cloudwatch_log_group" "measurement-validation-agent-lambda-log-group" {
  name = local.measurement_validation_agent_lambda_log_group
}

resource "aws_cloudwatch_log_stream" "measurement-validation-agent-lambda-log-stream" {
  name           = local.measurement_validation_agent_lambda_stream_name
  log_group_name = aws_cloudwatch_log_group.measurement-validation-agent-lambda-log-group.name
}

resource "aws_iam_role_policy" "measurement_validation_agent_access_policy" {
  name = "measurement_validation_agent_lambda_access_policy"
  role = aws_iam_role.measurement_validation_agent_lambda_iam.name
  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowToAssumeRole",
            "Effect": "Allow",
            "Action": [
                "sts:AssumeRole"
            ],
            "Resource": "*"
        }
    ]
}
EOF
}

resource "aws_iam_role" "measurement_validation_agent_lambda_iam" {
  name = "measurement_validation_agent_lambda-iam${var.tag_postfix}"

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

resource "aws_lambda_function" "measurement_validation_agent_lambda" {
  function_name    = var.measurement_validation_agent_lambda_function_name
  role             = aws_iam_role.measurement_validation_agent_lambda_iam.arn
  handler          = "measurement_validation_runner.lambda_handler"
  runtime          = "python3.9"
  s3_bucket        = var.measurement_validation_agent_lambda_source_bucket
  s3_key           = var.measurement_validation_agent_lambda_s3_key
  memory_size      = 500
  timeout          = 900
  publish          = true
  environment {
    variables = {
      DEBUG = "false",
      encrypted_file_bucket = var.measurement_validation_agent_lambda_input_bucket
    }
  }

  depends_on = [aws_s3_bucket_object.upload_lambda]
}
