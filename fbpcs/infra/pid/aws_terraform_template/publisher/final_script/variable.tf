variable "aws_region" {
  type        = string
  description = "region of the aws resources"
  default     = "us-west-1"
}

variable "pid_id" {
  type        = string
  description = "The identifier for marking the cloud resources in MR PID"
}

variable "pce_instance_id" {
  type        = string
  description = "Publisher PCE instance ID"
}

variable "partner_account_id" {
  type        = string
  description = "Partner AWS account ID"
}

variable "partner_unique_tag" {
  type        = string
  description = "Partner Deployment unique Tag"
}
