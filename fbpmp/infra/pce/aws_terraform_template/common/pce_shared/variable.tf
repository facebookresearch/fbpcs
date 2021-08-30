variable "aws_region" {
  description = "region of the aws resources"
  default     = "us-west-1"
}

variable "tag_postfix" {
  description = "the postfix to append after a resource name or tag"
  default     = ""
}

variable "onedocker_ecs_container_image" {
  description = "the ECR URI of the image to be loaded in the onedocker container. E.g. 539290649537.dkr.ecr.us-west-2.amazonaws.com/one-docker-prod:latest"
  default     = ""
}

variable "aws_account_id" {
  description = "your aws account id, that's used to create the task_execution_role and task_role"
  default     = ""
}

variable "pce_id" {
  type        = string
  description = "The identifier for marking the cloud resources are in PCE"
}
