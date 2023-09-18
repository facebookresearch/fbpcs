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

variable "measurement_validation_agent_lambda_function_name" {
  description = "Name of the Measurement validation Agent lambda"
  default     = ""
}

variable "measurement_validation_agent_lambda_source_bucket" {
  description = "S3 bucket where source code zip file for Measurement validation Agent is stored."
  default     = ""
}

variable "measurement_validation_agent_lambda_input_bucket" {
  description = "S3 bucket where input data for Measurement validation Agent is stored."
  default     = ""
}

variable "measurement_validation_agent_lambda_s3_key" {
  description = "S3 key for source code zip file for Measurement validation Agent."
  default     = ""
}
