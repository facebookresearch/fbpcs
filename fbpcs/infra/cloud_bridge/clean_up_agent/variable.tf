variable "region" {
  description = "region of the aws resources"
  default     = "us-west-2"
}

variable "tag_postfix" {
  description = "the postfix to append after a resource name or tag"
  default     = ""
}

variable "aws_account_id" {
  description = "your aws account id, that's used to read encrypted S3 files"
  default     = ""
}

variable "clean_up_agent_lambda_function_name" {
  description = "Name of the Clean Up Agent lambda"
  default     = ""
}

variable "clean_up_agent_lambda_source_bucket" {
  description = "S3 bucket where source code zip file for Clean Up Agent is stored."
  default     = ""
}

variable "clean_up_agent_lambda_input_bucket" {
  description = "S3 bucket where input data for Clean Up Agent is stored."
  default     = ""
}

variable "clean_up_agent_lambda_s3_key" {
  description = "S3 key for source code zip file for Clean Up Agent."
  default     = ""
}
