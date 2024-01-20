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

## Get the advertiser infra common kinesis Log Stream
data "aws_kinesis_stream" "logs_kinesis_stream" {
  name = var.kinesis_log_stream_name
}

## Get the common cloudwatch log group
data "aws_cloudwatch_log_group" "cloudtrail_logs" {
  name = "/aws/cloudtrail/${var.installation_tag}-cw-logs"
}

## Get IAM Role of common cloudWatch to publish logs to Kinesis
data "aws_iam_role" "cloudwatch_kinesis_logging" {
  name = "${var.installation_tag}-cw-role"
}

## Push kms cloudwatch log group to Kinesis stream
resource "aws_cloudwatch_log_subscription_filter" "cloudwatch_log_to_kinesis_subscription" {
  name            = "${var.installation_tag}-kms-log-filter"
  log_group_name  = data.aws_cloudwatch_log_group.cloudtrail_logs.name
  filter_pattern  = "{($.eventSource = \"kms.amazonaws.com\") && ($.userIdentity.principalId = %${var.user_identity_regex}%)}"
  destination_arn = data.aws_kinesis_stream.logs_kinesis_stream.arn
  role_arn        = data.aws_iam_role.cloudwatch_kinesis_logging.arn
}
