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
  source_file = "data_transformation_lambda.py"
  output_path = "lambda.zip"
}

resource "aws_s3_bucket_object" "upload_lambda" {
  bucket = var.data_processing_lambda_s3_bucket
  key    = var.data_processing_lambda_s3_key
  source = "lambda.zip"
  etag   = filemd5("data_transformation_lambda.py")
}

resource "aws_kms_key" "s3_kms_key" {
  description = "This key is used to encrypt bucket objects"
}

resource "aws_kinesis_firehose_delivery_stream" "extended_s3_stream" {
  name        = "cb-data-ingestion-stream${var.tag_postfix}"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose_role.arn
    bucket_arn          = aws_s3_bucket.bucket.arn
    buffer_size         = 128
    buffer_interval     = 900
    prefix              = "year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    kms_key_arn         = aws_kms_key.s3_kms_key.arn
    processing_configuration {
      enabled = "true"

      processors {
        type = "Lambda"

        parameters {
          parameter_name  = "LambdaArn"
          parameter_value = "${aws_lambda_function.lambda_processor.arn}:$LATEST"
        }
      }
    }
  }
}

resource "aws_s3_bucket" "bucket" {
  bucket = var.data_processing_output_bucket
  versioning {
    enabled = true
  }
  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        kms_master_key_id = aws_kms_key.s3_kms_key.arn
        sse_algorithm     = "aws:kms"
      }
      bucket_key_enabled = true
    }
  }

}

resource "aws_s3_bucket_policy" "bucket_policy" {
  bucket = aws_s3_bucket.bucket.id

  policy = <<EOF
{
  "Statement": [
    {
      "Effect": "Deny",
      "Action": "s3:*",
      "Principal": "*",
      "Resource": [
        "${aws_s3_bucket.bucket.arn}",
        "${aws_s3_bucket.bucket.arn}/*"
      ],
      "Condition": {
        "Bool": { "aws:SecureTransport": false }
      }
    }
  ]
}
EOF
}

resource "aws_iam_role" "firehose_role" {
  name = "cb-data-ingestion-firehose-role${var.tag_postfix}"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "firehose.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "attach_lambda_access" {
  name   = "lambda-policy${var.tag_postfix}"
  role   = aws_iam_role.firehose_role.id
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:*"
      ],
      "Resource": [
        "arn:aws:lambda:${var.region}:${var.aws_account_id}:function:${aws_lambda_function.lambda_processor.function_name}:*"
      ]
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "attach_s3_access" {
  name   = "s3-policy${var.tag_postfix}"
  role   = aws_iam_role.firehose_role.id
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:*"
      ],
      "Resource": [
        "arn:aws:s3:::${var.data_processing_output_bucket}",
        "arn:aws:s3:::${var.data_processing_output_bucket}/*"
      ]
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "kms_policy_firehose" {
  name   = "firehose-kms-policy${var.tag_postfix}"
  role   = aws_iam_role.firehose_role.id
  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "kms:*",
      "Resource": [
        "arn:aws:kms:*:${var.aws_account_id}:key/*",
        "arn:aws:kms:*:${var.aws_account_id}:alias/*"
      ]
    }
  ]
}
EOF
}

resource "aws_iam_role" "lambda_iam" {
  name = "lambda-iam${var.tag_postfix}"

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

resource "aws_iam_role_policy_attachment" "lambda_kinesis_role" {
  role       = aws_iam_role.lambda_iam.id
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole"
}

resource "aws_lambda_function" "lambda_processor" {
  s3_bucket     = var.data_processing_lambda_s3_bucket
  s3_key        = var.data_processing_lambda_s3_key
  function_name = "cb-data-ingestion-stream-processor${var.tag_postfix}"
  role          = aws_iam_role.lambda_iam.arn
  handler       = "data_transformation_lambda.lambda_handler"
  runtime       = "python3.8"
  timeout       = 60
  environment {
    variables = {
      DEBUG = "false"
    }
  }
}
