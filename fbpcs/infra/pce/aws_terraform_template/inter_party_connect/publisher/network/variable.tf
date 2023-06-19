variable "aws_region" {
  description = "region of the aws resources"
  default     = "us-west-1"
}

variable "tag_postfix" {
  description = "the postfix to append after a resource name or tag"
  default     = ""
}
