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
  source_dir = "ingestion_input_data_validation_source_code"
  output_path = "ingestion_input_data_validation.zip"
}

resource "aws_s3_bucket_object" "upload_lambda" {
  bucket = var.ingestion_input_data_validation_lambda_s3_bucket
  key    = var.ingestion_input_data_validation_lambda_s3_key
  source = "ingestion_input_data_validation.zip"
}

locals {
  ingestion_input_data_validation_lambda_log_group       = "/aws/lambda/${var.ingestion_input_data_validation_lambda_function_name}"
  ingestion_input_data_validation_lambda_stream_name = "ingestion-input-data-validation-lambda-log-stream"
}

resource "aws_cloudwatch_log_group" "ingestion_input_data_validation_lambda_log_group" {
  name = local.ingestion_input_data_validation_lambda_log_group
}

resource "aws_cloudwatch_log_stream" "ingestion_input_data_validation_lambda_log_stream" {
  name           = local.ingestion_input_data_validation_lambda_stream_name
  log_group_name = aws_cloudwatch_log_group.ingestion_input_data_validation_lambda_log_group.name
}

resource "aws_iam_role_policy" "ingestion_input_data_validation_lambda_access_policy" {
  name = "ingestion_input_data_validation_lambda_access_policy"
  role = aws_iam_role.ingestion_input_data_validation_lambda_iam.name
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowLambdaAccessToS3",
      "Effect": "Allow",
      "Action": [
         "s3:GetObject",
         "s3:PutObject",
         "s3:ListObjects"
      ],
      "Resource":[
        "arn:aws:s3:::${var.ingestion_input_data_validation_lambda_input_bucket}",
        "arn:aws:s3:::${var.ingestion_input_data_validation_lambda_input_bucket}/*"
      ]
    },
    {
      "Sid": "AllowLambdaAccessToCloudWatch",
      "Effect": "Allow",
      "Action": [
         "logs:CreateLogGroup",
         "logs:CreateLogStream",
         "logs:PutLogEvents"
      ],
      "Resource": "*"
    },
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

resource "aws_iam_role" "ingestion_input_data_validation_lambda_iam" {
  name = "ingestion-input-data-validation-lambda-iam${var.tag_postfix}"

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

resource "aws_lambda_function" "ingestion_input_data_validation_lambda" {
  function_name    = var.ingestion_input_data_validation_lambda_function_name
  role             = aws_iam_role.ingestion_input_data_validation_lambda_iam.arn
  handler          = "ingestion_input_data_validation_runner.lambda_handler"
  runtime          = "python3.9"
  s3_bucket        = var.ingestion_input_data_validation_lambda_s3_bucket
  s3_key           = var.ingestion_input_data_validation_lambda_s3_key
  memory_size      = 2048
  timeout          = 900
  publish          = true
  environment {
    variables = {
      DEBUG = "false",
    }
  }

  depends_on = [aws_s3_bucket_object.upload_lambda]
}

resource "aws_lambda_permission" "allow_bucket_invoke" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_input_data_validation_lambda.arn
  principal     = "s3.amazonaws.com"
  source_arn    = var.data_bucket_arn
}
