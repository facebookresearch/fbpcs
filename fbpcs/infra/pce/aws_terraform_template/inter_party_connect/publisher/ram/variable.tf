variable "aws_region" {
  description = "region of the aws resources"
  default     = "us-west-1"
}

variable "tag_postfix" {
  description = "the postfix to append after a resource name or tag"
  default     = ""
}

variable "transit_gateway_arn" {
  description = "Amazon Resource Name (ARN) of the transit gateway to associate with the RAM Resource Share"
}

variable "principal" {
  description = "The principal to associate with the resource share. Possible values are an AWS account ID, an AWS Organizations Organization ARN, or an AWS Organizations Organization Unit ARN"
}
