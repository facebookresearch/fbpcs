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

## Create the cloudwatch log group name for TEE-PL data S3 buckets 
locals {
  s3_cloudwatch_log_group = "/aws/cloudtrail/s3/${var.s3_bucket_name}"
}

## Get the existing resource for TEE-PL data S3 bucket
data "aws_s3_bucket" "s3_bucket" {
  bucket = var.s3_bucket_name
}

## Get the existing resource for S3 logging bucket
data "aws_s3_bucket" "s3_logging_bucket" {
  bucket = var.s3_logging_bucket_name
}

## Get the advertiser infra common kinesis Log Stream
data "aws_kinesis_stream" "logs_kinesis_stream" {
  name = var.kinesis_log_stream_name
}

## Create a cloudwatch S3 data event log group
resource "aws_cloudwatch_log_group" "cloudtrail_s3_logs" {
  name              = local.s3_cloudwatch_log_group
  retention_in_days = 7
}

## Setup s3 cloudtrail iam role and policies to write to cloudwatch log group
resource "aws_iam_role" "cloudtrail" {
  name = "${var.s3_bucket_name}-cloudtrail-role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "cloudtrail.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}


resource "aws_iam_role_policy" "cloudwatch_logs" {
  name = "${var.s3_bucket_name}-cloudtrail-cloudwatch-policy"
  role = aws_iam_role.cloudtrail.id

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {   
            "Sid": "AWSCloudTrailCreateLogs",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],  
            "Resource": [
                "${aws_cloudwatch_log_group.cloudtrail_s3_logs.arn}:*"
            ]   
        }  
    ]   
}
EOF
}


## Create IAM Role for CloudWatch to publish logs to Kinesis
resource "aws_iam_role" "cloudwatch_log" {
  name = "${var.s3_bucket_name}_cloudwatch_log_role"

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

## Create IAM Policy for CloudWatch to publish logs to the advertiser infra commot Kinesis stream
resource "aws_iam_role_policy" "kinesis_write_policy" {
  name = "${var.s3_bucket_name}_cloudwatch_kinesis_write_policy"
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



## Setup S3 bucket logs to cloudtrail to cloudwatch pipe
resource "aws_cloudtrail" "cloudtrail_s3_cloudwatch_logging" {
  name                       = "${var.s3_bucket_name}-cloudtrail-s3-cloudwatch-logging"
  s3_bucket_name             = var.s3_logging_bucket_name
  s3_key_prefix              = "${var.s3_bucket_name}_logs"
  cloud_watch_logs_role_arn  = aws_iam_role.cloudtrail.arn
  cloud_watch_logs_group_arn = "${aws_cloudwatch_log_group.cloudtrail_s3_logs.arn}:*"

  event_selector {
    read_write_type           = "WriteOnly"
    include_management_events = false

    data_resource {
      type   = "AWS::S3::Object"
      values = ["${data.aws_s3_bucket.s3_bucket.arn}/"]
    }
  }
  depends_on = [aws_cloudwatch_log_group.cloudtrail_s3_logs]
}


## Push s3 bucket cloudwatch log group to Kinesis stream
resource "aws_cloudwatch_log_subscription_filter" "cloudwatch_log_to_kinesis_subscription" {
  name            = "${var.s3_bucket_name}-cloudwatch-log-to-kinesis-subscription"
  log_group_name  = aws_cloudwatch_log_group.cloudtrail_s3_logs.name
  filter_pattern  = "" # forward all logs to kinesis stream
  destination_arn = data.aws_kinesis_stream.logs_kinesis_stream.arn
  role_arn        = aws_iam_role.cloudwatch_log.arn
}
