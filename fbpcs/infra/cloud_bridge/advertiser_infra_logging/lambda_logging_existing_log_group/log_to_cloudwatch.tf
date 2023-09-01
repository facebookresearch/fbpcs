provider "aws" {
  region = var.region
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

locals {
  lambda_cloudwatch_log_group = "/aws/lambda/${var.lambda_name}"
}

## Get Kinesis Log Stream
data "aws_kinesis_stream" "logs_kinesis_stream" {
  name = var.kinesis_log_stream_name
}

## Find existing cloudwatch lambda log group if it exists
data "aws_cloudwatch_log_group" "lambda_logs" {
  name = local.lambda_cloudwatch_log_group
}

## Find the existing lambda log function
data "aws_lambda_function" "lambda" {
  function_name = var.lambda_name
}

## Extract the lambda role name from the ARN
locals {
  existing_lambda_role_name = regex("arn:aws:iam::\\d+:role/(.+)", data.aws_lambda_function.lambda.role)[0]
}

## Create a policy for lambda to be able to write logs to cloudwatch log group
resource "aws_iam_policy" "cloudwatch_logs" {
  name = "${var.lambda_name}-cpol"

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {   
            "Sid": "AWSLambdaCreateLogs",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],  
            "Resource": [
                "${data.aws_cloudwatch_log_group.lambda_logs.arn}:*"
            ]   
        }  
    ]   
}
EOF
}


## Attach the cloudwatch log write policy to the running lambda 
resource "aws_iam_role_policy_attachment" "updated_lambda_role_policy_attachment" {
  policy_arn = aws_iam_policy.cloudwatch_logs.arn
  role       = local.existing_lambda_role_name
}


## Create IAM Role for CloudWatch to publish logs to Kinesis
resource "aws_iam_role" "cloudwatch_log" {
  name = "${var.lambda_name}_crol"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "logs.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

## Create IAM Policy for CloudWatch to publish logs to Kinesis
resource "aws_iam_role_policy" "kinesis_write_policy" {
  name = "${var.lambda_name}_kpol"
  role = aws_iam_role.cloudwatch_log.id

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {   
            "Sid": "AWSCloudwatchKinesisWriteLogs",
            "Effect": "Allow",
            "Action": [
                "kinesis:PutRecord"
            ],  
            "Resource": [
                "${data.aws_kinesis_stream.logs_kinesis_stream.arn}"
            ]   
        }  
    ]   
}
EOF
}


## Push lambda cloudwatch log group to Kinesis stream
resource "aws_cloudwatch_log_subscription_filter" "cloudwatch_log_to_kinesis_subscription" {
  name            = "${var.lambda_name}-sub"
  log_group_name  = local.lambda_cloudwatch_log_group
  filter_pattern  = "" # forward all lambda logs to kinesis stream
  destination_arn = data.aws_kinesis_stream.logs_kinesis_stream.arn
  role_arn        = aws_iam_role.cloudwatch_log.arn
}
