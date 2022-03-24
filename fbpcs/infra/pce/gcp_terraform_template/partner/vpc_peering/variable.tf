variable "project_id" {
  description = "an unique identifier for your GCP project"
}

variable "credential_path" {
  description = "credential path of GCP service accounts"
}

variable "name_postfix" {
  description = "the postfix to append after a resource name"
}

variable "vpc_id" {
  description = "The ip of provisioned vpc"
}

variable "otherparty_vpc_id" {
  description = "Other party's vpc id"
}
