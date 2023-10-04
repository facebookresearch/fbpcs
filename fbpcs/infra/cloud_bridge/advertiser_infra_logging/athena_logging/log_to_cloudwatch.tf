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

## create a cloudwatch log group name to capture Athena operations usage
locals {
  athena_cloudwatch_log_group = "/aws/athena/${var.installation_tag}"
}

## Get the existing resource for S3 logging bucket
data "aws_s3_bucket" "s3_logging_bucket" {
  bucket = var.s3_logging_bucket_name
}

## Get the advertiser infra common kinesis Log Stream
data "aws_kinesis_stream" "logs_kinesis_stream" {
  name = var.kinesis_log_stream_name
}

## Create a cloudwatch athena log group
resource "aws_cloudwatch_log_group" "cloudtrail_athena_logs" {
  name              = local.athena_cloudwatch_log_group
  retention_in_days = 7
}

### Setup athena cloudtrail iam role and policies to write to cloudwatch log group
resource "aws_iam_role" "cloudtrail_cloudwatch_role" {
  name = "${var.installation_tag}-athena-ct-role"

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
  name = "${var.installation_tag}-athena-ct-policy"
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
                "${aws_cloudwatch_log_group.cloudtrail_athena_logs.arn}:*"
            ]   
        }  
    ]   
}
EOF
}


### Setup athena logging for cloudtrail to cloudwatch log group
resource "aws_cloudtrail" "cloudtrail_athena_cloudwatch_logging" {
  name                       = "${var.installation_tag}-athena-trail"
  s3_bucket_name             = var.s3_logging_bucket_name
  s3_key_prefix              = "athena_logs"
  cloud_watch_logs_role_arn  = aws_iam_role.cloudtrail_cloudwatch_role.arn
  cloud_watch_logs_group_arn = "${aws_cloudwatch_log_group.cloudtrail_athena_logs.arn}:*"

  event_selector {
    read_write_type           = "All"
    include_management_events = true
  }
}

## Create IAM Role for CloudWatch to publish logs to Kinesis
resource "aws_iam_role" "cloudwatch_kinesis_role" {
  name = "${var.installation_tag}-athena-cw-role"

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

## Create IAM Policy for CloudWatch to publish athena logs to the advertiser infra common Kinesis stream
resource "aws_iam_role_policy" "cloudwatch_kinesis_write_policy" {
  name = "${var.installation_tag}-athena-cw-policy"
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

## Find all VPCs in the account that belongs to TEE-PL infra
data "aws_vpcs" "all_vpcs" {
  tags = {
    Application = "Private Computation Infrastructure"
  }
}

## Get details of all TEE-PL VPCs
data "aws_vpc" "filtered_vpc" {
  for_each = toset(data.aws_vpcs.all_vpcs.ids)
  id       = each.key
}

## Find all eks clusters in the account
data "aws_eks_clusters" "all_clusters" {}

## Get details of all the eks clusters in the account
data "aws_eks_cluster" "filtered_cluster" {
  for_each = toset(data.aws_eks_clusters.all_clusters.names)
  name     = each.key
}

## Extract the names of all TEE-PL VPCs
locals {
  vpc_name_list = [for vpc_name, params in data.aws_vpc.filtered_vpc : split("/", params.tags["Name"])[0]]
}

## Create a list of TEE-PL EKS cluster names
locals {
  tee_pl_eks_cluster_name_list = [for cluster_name, params in data.aws_eks_cluster.filtered_cluster : cluster_name if contains(local.vpc_name_list, params.tags["MainStack"])]
}

## Create the filter regex of TEE-PL EKS cluster names
locals {
  tee_pl_eks_cluster_name_regex = join("|", local.tee_pl_eks_cluster_name_list)
}

## Push athena cloudwatch log group to Kinesis stream
resource "aws_cloudwatch_log_subscription_filter" "cloudwatch_log_to_kinesis_subscription" {
  name            = "${var.installation_tag}-athena-log-filter"
  log_group_name  = aws_cloudwatch_log_group.cloudtrail_athena_logs.name
  filter_pattern  = "{($.eventSource = \"athena.amazonaws.com\") && ($.userIdentity.sessionContext.sessionIssuer.userName = %^eksctl-.*${local.tee_pl_eks_cluster_name_regex}.*%)}"
  destination_arn = data.aws_kinesis_stream.logs_kinesis_stream.arn
  role_arn        = aws_iam_role.cloudwatch_kinesis_role.arn
}
