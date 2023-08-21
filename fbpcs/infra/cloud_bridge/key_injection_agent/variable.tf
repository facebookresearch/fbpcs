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

variable "kia_lambda_function_name" {
  description = "Name of the KIA lambda"
  default     = ""
}

variable "kia_lambda_s3_bucket" {
  description = "S3 bucket where source code zip file for KIA is stored."
  default     = ""
}

variable "kia_lambda_input_bucket" {
  description = "S3 bucket where input data for KIA is stored."
  default     = ""
}

variable "kia_lambda_s3_key" {
  description = "S3 key for source code zip file for KIA."
  default     = ""
}

variable "clean_up_agent_lambda_iam_role" {
  description = "IAM Role arn of the clean up agent lambda."
  default     = ""
}
