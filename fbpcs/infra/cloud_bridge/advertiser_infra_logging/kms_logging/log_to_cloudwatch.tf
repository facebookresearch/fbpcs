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

## capture the cloudwatch log group name for TEE-PL kms key usage
locals {
  kms_cloudwatch_log_group = "/aws/kms/${var.installation_tag}"
}

## Get the existing resource for S3 logging bucket
data "aws_s3_bucket" "s3_logging_bucket" {
  bucket = var.s3_logging_bucket_name
}

## Get the advertiser infra common kinesis Log Stream
data "aws_kinesis_stream" "logs_kinesis_stream" {
  name = var.kinesis_log_stream_name
}

## Create a cloudwatch kms key event log group
resource "aws_cloudwatch_log_group" "cloudtrail_kms_logs" {
  name              = local.kms_cloudwatch_log_group
  retention_in_days = 7
}

### Setup kms cloudtrail iam role and policies to write to cloudwatch log group
resource "aws_iam_role" "cloudtrail_cloudwatch_role" {
  name = "${var.installation_tag}-kms-ct-role"

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

resource "aws_iam_role_policy" "cloudtrail_cloudwatch_write_policy" {
  name = "${var.installation_tag}-kms-ct-policy"
  role = aws_iam_role.cloudtrail_cloudwatch_role.id

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
                "${aws_cloudwatch_log_group.cloudtrail_kms_logs.arn}:*"
            ]   
        }  
    ]   
}
EOF
}


### Setup kms logging for cloudtrail to cloudwatch log group
resource "aws_cloudtrail" "cloudtrail_kms_cloudwatch_logging" {
  name                       = "${var.installation_tag}-kms-trail"
  s3_bucket_name             = var.s3_logging_bucket_name
  s3_key_prefix              = "kms_logs"
  cloud_watch_logs_role_arn  = aws_iam_role.cloudtrail_cloudwatch_role.arn
  cloud_watch_logs_group_arn = "${aws_cloudwatch_log_group.cloudtrail_kms_logs.arn}:*"

  event_selector {
    read_write_type           = "All"
    include_management_events = true
  }
}

## Create IAM Role for CloudWatch to publish logs to Kinesis
resource "aws_iam_role" "cloudwatch_kinesis_role" {
  name = "${var.installation_tag}-kms-cw-role"

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

## Create IAM Policy for CloudWatch to publish kms logs to the advertiser infra common Kinesis stream
resource "aws_iam_role_policy" "cloudwatch_kinesis_write_policy" {
  name = "${var.installation_tag}-kms-cw-policy"
  role = aws_iam_role.cloudwatch_kinesis_role.id

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

## Push kms cloudwatch log group to Kinesis stream
resource "aws_cloudwatch_log_subscription_filter" "cloudwatch_log_to_kinesis_subscription" {
  name           = "${var.installation_tag}-kms-log-filter"
  log_group_name = aws_cloudwatch_log_group.cloudtrail_kms_logs.name
  filter_pattern  = "{($.eventSource = \"kms.amazonaws.com\") && ($.userIdentity.principalId = %${var.user_identity_regex}%)}"
  destination_arn = data.aws_kinesis_stream.logs_kinesis_stream.arn
  role_arn        = aws_iam_role.cloudwatch_kinesis_role.arn
}
