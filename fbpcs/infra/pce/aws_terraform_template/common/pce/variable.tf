variable "aws_region" {
  description = "region of the aws resources"
  default     = "us-west-1"
}

variable "tag_postfix" {
  description = "the postfix to append after a resource name or tag"
  default     = ""
}

variable "vpc_cidr" {
  description = "VPC's CIDR block, it should not overlap with existing VPCs' CIDR"
  default     = "10.1.0.0/16"
}

variable "otherparty_vpc_cidr" {
  description = "Other party's VPC's CIDR block, it should not overlap with existing VPCs' CIDR"
  default     = "10.0.0.0/16"
}

variable "ingress_rules" {
  default = {}
  type = map(object({
    description = string
    from_port   = number
    to_port     = number
    protocol    = string
    cidr_blocks = list(string)
  }))
  description = "Security group ingress rules"
}

variable "pce_id" {
  type        = string
  description = "The identifier for marking the cloud resources are in PCE"
}

variable "vpc_logging" {
  description = "An object which configures VPC logging"
  type = object({
    enabled    = bool
    bucket_arn = string
  })
  default = {
    enabled    = false
    bucket_arn = ""
  }

  validation {
    condition     = var.vpc_logging.enabled == false || var.vpc_logging.bucket_arn != ""
    error_message = "An S3 bucket ARN must be provided if VPC logging is enabled (ex. 'arn:aws:s3:::YOUR-BUCKET-NAME')."
  }
}
