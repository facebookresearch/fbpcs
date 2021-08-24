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
