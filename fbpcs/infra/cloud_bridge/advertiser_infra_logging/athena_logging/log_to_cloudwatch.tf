provider "aws" {
  region = var.region
}

provider "archive" {}

terraform {
  backend "s3" {}
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
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
  tee_pl_filtered_eks_clusters = [for cluster in data.aws_eks_cluster.filtered_cluster : cluster if can(cluster.tags["MainStack"])]
  tee_pl_eks_cluster_name_list = [for cluster in local.tee_pl_filtered_eks_clusters : cluster.name if contains(local.vpc_name_list, cluster.tags["MainStack"])]
}

## Create the filter regex of TEE-PL EKS cluster names
locals {
  tee_pl_eks_cluster_name_regex = join("|", local.tee_pl_eks_cluster_name_list)
}

## Push athena/glue cloudwatch log group to Kinesis stream
resource "aws_cloudwatch_log_subscription_filter" "cloudwatch_log_to_kinesis_subscription" {
  count           = local.tee_pl_eks_cluster_name_regex != "" ? 1 : 0
  name            = "${var.installation_tag}-athena-log-filter"
  log_group_name  = data.aws_cloudwatch_log_group.cloudtrail_logs.name
  filter_pattern  = "{($.eventSource = \"athena.amazonaws.com\" || $.eventSource = \"glue.amazonaws.com\") && ($.userIdentity.sessionContext.sessionIssuer.userName = %^eksctl-.*${local.tee_pl_eks_cluster_name_regex}.*%)}"
  destination_arn = data.aws_kinesis_stream.logs_kinesis_stream.arn
  role_arn        = data.aws_iam_role.cloudwatch_kinesis_logging.arn
}
