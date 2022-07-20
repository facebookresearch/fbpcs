variable "gcp_region" {
  description = "region of the gcp resources"
  default     = "us-west2"
}

variable "project_id" {
  description = "an unique identifier for your GCP project"
}

variable "name_postfix" {
  description = "the postfix to append after a resource name"
}

variable "subnet_primary_cidr" {
  description = "the primary CIDR block of a subnet"
  default     = "10.10.0.0/20"
}

variable "subnet_secondary_cidr" {
  description = "the secondary CIDR block of a subnet"
  default     = "10.1.0.0/20"
}

variable "otherparty_subnet_cidr" {
  description = "Other party's subnet's secondary CIDR block, it should not overlap with existing subnets' 2nd CIDR"
  default     = "10.0.0.0/20"
}
