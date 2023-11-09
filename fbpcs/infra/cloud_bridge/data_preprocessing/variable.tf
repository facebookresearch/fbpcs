variable "region" {
  description = "region of the aws resources"
  default     = "us-west-2"
}

variable "tag_postfix" {
  description = "the postfix to append after a resource name or tag"
  default     = ""
}

variable "aws_account_id" {
  description = "your aws account id"
  default     = ""
}

variable "ingestion_input_data_validation_lambda_function_name" {
  description = "Name of the ingestion input data validation lambda"
  default     = ""
}

variable "ingestion_input_data_validation_lambda_s3_bucket" {
  description = "S3 bucket where source code zip file for ingestion input data validation is stored."
  default     = ""
}

variable "ingestion_input_data_validation_lambda_input_bucket" {
  description = "S3 bucket where input data for ingestion input data validation is stored."
  default     = ""
}

variable "ingestion_input_data_validation_lambda_s3_key" {
  description = "S3 key for source code zip file for ingestion input data validation."
  default     = ""
}

variable "data_bucket_arn" {
  description = "The ARN of the S3 bucket for advertisers to upload data and necessary python scripts"
  default     = ""
}
