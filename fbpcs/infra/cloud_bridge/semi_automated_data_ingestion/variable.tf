variable "region" {
  description = "region of the aws resources"
  default     = "us-west-2"
}

variable "app_data_input_bucket" {
  description = "S3 bucket for advertisers to upload app data and necessary python scripts"
  default     = ""
}

variable "app_data_input_bucket_id" {
  description = "The ID of the S3 bucket for advertisers to upload app data and necessary python scripts"
  default     = ""
}

variable "app_data_input_bucket_arn" {
  description = "The ARN of the S3 bucket for advertisers to upload app data and necessary python scripts"
  default     = ""
}

variable "lambda_trigger_s3_key" {
  description = "Source S3 key for lambda trigger function used in semi-automated data ingestion"
  default     = ""
}

variable "tag_postfix" {
  description = "the postfix to append after a resource name or tag"
  default     = ""
}

variable "aws_account_id" {
  description = "your aws account id, that's used to read encrypted S3 files"
  default     = ""
}

variable "data_upload_key_path" {
  description = "S3 key path where events data will be uploaded"
  default     = "semi-automated-data-ingestion"
}
