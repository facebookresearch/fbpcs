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
  source_dir = "clean_up_agent_source_code"
  output_path = "source.zip"
}

resource "aws_s3_bucket_object" "upload_lambda" {
  bucket = var.clean_up_agent_lambda_source_bucket
  key    = var.clean_up_agent_lambda_s3_key
  source = "source.zip"
}

locals {
  clean_up_agent_lambda_log_group       = "/aws/lambda/${var.clean_up_agent_lambda_function_name}"
  clean_up_agent_lambda_stream_name     = "clean-up-agent-lambda-log-stream"
}

resource "aws_cloudwatch_log_group" "clean-up-agent-lambda-log-group" {
  name = local.clean_up_agent_lambda_log_group
}

resource "aws_cloudwatch_log_stream" "clean-up-agent-lambda-log-stream" {
  name           = local.clean_up_agent_lambda_stream_name
  log_group_name = aws_cloudwatch_log_group.clean-up-agent-lambda-log-group.name
}

resource "aws_iam_role_policy" "clean_up_agent_lambda_access_policy" {
  name = "clean_up_agent_lambda_access_policy"
  role = aws_iam_role.clean_up_agent_lambda_iam.name
  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowLambdaAccessToS3",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:ListBucketVersions",
                "s3:ListObjectsV2",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::${var.clean_up_agent_lambda_input_bucket}",
                "arn:aws:s3:::${var.clean_up_agent_lambda_input_bucket}/*"
            ]
        },
        {
            "Sid": "AllowLambdaAccessToModifyS3BucketPolicy",
            "Effect": "Allow",
            "Action": [
              "s3:GetBucketAcl",
              "s3:PutBucketPolicy"
            ],
            "Resource": "arn:aws:s3:::${var.clean_up_agent_lambda_input_bucket}"
        },
        {
            "Sid": "AllowLambdaAccessToKMSKey",
            "Effect": "Allow",
            "Action": [
                "kms:DescribeKey",
                "kms:GetKeyPolicy",
                "kms:ScheduleKeyDeletion"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "aws:ResourceTag/Name": "CreatedBy",
                    "aws:ResourceTag/Value": "KIALambda"
                }
            }
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
        }
    ]
}
EOF
}

resource "aws_iam_role" "clean_up_agent_lambda_iam" {
  name = "clean_up_agent_lambda-iam${var.tag_postfix}"

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

resource "aws_lambda_function" "clean_up_agent_lambda" {
  function_name    = var.clean_up_agent_lambda_function_name
  role             = aws_iam_role.clean_up_agent_lambda_iam.arn
  handler          = "pl_run_clean_up.lambda_handler"
  runtime          = "python3.9"
  s3_bucket        = var.clean_up_agent_lambda_source_bucket
  s3_key           = var.clean_up_agent_lambda_s3_key
  memory_size      = 500
  timeout          = 900
  publish          = true
  environment {
    variables = {
      DEBUG = "false",
      encrypted_file_bucket = var.clean_up_agent_lambda_input_bucket
    }
  }

  depends_on = [aws_s3_bucket_object.upload_lambda]
}

resource "aws_cloudwatch_event_rule" "clean_up_agent_schedule_rule" {
  name        = "pc-clean-up-schedule-rule${var.tag_postfix}"
  description = "PC Clean up Schedule Rule."
  schedule_expression = "rate(1 hour)"
}

resource "aws_cloudwatch_event_target" "clean_up_agent_schedule_target" {
  rule      = aws_cloudwatch_event_rule.clean_up_agent_schedule_rule.name
  target_id = "LambdaFunction"
  arn       = aws_lambda_function.clean_up_agent_lambda.arn
  input     = jsonencode({})
}

resource "aws_lambda_permission" "allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.clean_up_agent_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.clean_up_agent_schedule_rule.arn
}
