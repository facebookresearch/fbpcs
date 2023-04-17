variable "aws_region" {
  type        = string
  description = "region of the aws resources"
  default     = "us-west-1"
}

variable "pid_id" {
  type        = string
  description = "The identifier for marking the cloud resources in MR PID"
}

variable "partner_account_id" {
  type        = string
  description = "Partner AWS account ID"
}

variable "md5hash_partner_account_id" {
  type        = string
  description = "MD5 hashed Partner AWS account ID"
}
